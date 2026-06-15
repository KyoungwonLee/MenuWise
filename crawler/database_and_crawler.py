import sqlite3
import math
import hashlib
import json
import os
import re
import threading
import time
from urllib.parse import urlencode
from urllib.request import Request, urlopen


DEFAULT_MENU_PRICE = 0
APIFY_REVIEW_LIMIT_PER_PLACE = 40
APIFY_PHOTO_LIMIT_PER_PLACE = 30

# AI가 "리뷰에 언급이 없다 / 정보가 부족하다"는 식으로 만들어 내는 내용 없는 문장들.
# 실제 리뷰에서 뽑은 장단점이 아니므로 core_info에 저장하지 않는다(프론트는 '리뷰 없음' 안내를 띄운다).
FILLER_CORE_INFO_MARKERS = (
    "언급되지", "언급이 없", "언급은 없", "언급 없", "언급은 없었", "언급이 없었",
    "정보가 부족", "정보가 없", "정보는 없", "정보에 대한 평가가 어렵",
    "확인되지", "확인할 수 없", "알 수 없", "찾을 수 없",
    "나타나지 않", "드러나지 않", "파악하기 어렵", "파악하기 힘들",
    "특별한 단점", "특별한 장점", "별다른 단점", "별다른 장점",
    "언급한 내용이 없", "언급된 내용이 없", "없어 정보", "평가가 어렵", "판단하기 어렵",
)


def is_filler_core_info(content):
    """내용 없는 '언급 없음/정보 부족' 류의 문장이면 True."""
    text = str(content or "")
    return any(marker in text for marker in FILLER_CORE_INFO_MARKERS)


def load_apify_reviews_from_file(path):
    """Apify Dataset에서 내려받은 JSON 파일을 읽는다."""
    with open(path, "r", encoding="utf-8") as file:
        data = json.load(file)

    if isinstance(data, dict):
        return data.get("items", [])

    if isinstance(data, list):
        return data

    return []


def group_apify_reviews_by_place(items):
    """Apify 리뷰 단위 output을 Google place_id 기준으로 묶는다."""
    grouped = {}

    for item in items:
        place_id = item.get("place_id")
        if not place_id:
            continue

        grouped.setdefault(place_id, []).append(item)

    return grouped


def convert_apify_groups_to_restaurant_data(
    grouped_items,
    review_limit=APIFY_REVIEW_LIMIT_PER_PLACE,
    photo_limit=APIFY_PHOTO_LIMIT_PER_PLACE,
):
    """Apify 리뷰 output을 MenuWiseDB.save_restaurant_data 입력 형식으로 변환한다."""
    restaurants = []

    for place_id, reviews in grouped_items.items():
        if not reviews:
            continue

        first = reviews[0]
        res_id = f"GOOGLE_{place_id}"
        menu_id = f"{res_id}_MENU"
        place_name = first.get("place_name") or "이름 없는 식당"
        location = first.get("location") or {}
        photo_urls = _collect_apify_photo_urls(reviews, photo_limit)

        restaurants.append({
            "restaurant": {
                "res_id": res_id,
                "res_name": place_name,
                "lat": location.get("lat"),
                "lng": location.get("lng"),
                "category": _first_value(first.get("categories")) or first.get("category", ""),
            },
            "menus": [
                {
                    "menu_id": menu_id,
                    "menu_name": f"{place_name} 대표 메뉴",
                    "price": DEFAULT_MENU_PRICE,
                    "photo_url": photo_urls[0] if photo_urls else first.get("place_photo_url", ""),
                }
            ],
            "reviews": _convert_apify_reviews(res_id, menu_id, reviews, review_limit),
            "source": {
                "provider": "apify_google_maps_reviews",
                "place_id": place_id,
                "place_rating": first.get("place_rating"),
                "place_reviews_count": first.get("place_reviews_count"),
                "photo_urls": photo_urls,
                "full_address": first.get("full_address", ""),
            },
        })

    return restaurants


def _collect_apify_photo_urls(reviews, limit):
    photo_urls = []
    seen = set()

    for review in reviews:
        candidates = []
        if review.get("place_photo_url"):
            candidates.append(review["place_photo_url"])
        candidates.extend(review.get("review_photos_urls") or [])

        for url in candidates:
            if not url or url in seen:
                continue

            seen.add(url)
            photo_urls.append(url)

            if len(photo_urls) >= limit:
                return photo_urls

    return photo_urls


def _convert_apify_reviews(res_id, menu_id, reviews, limit):
    converted = []
    seen = set()

    for index, review in enumerate(reviews):
        content = str(review.get("content") or "").strip()
        if not content or content in seen:
            continue

        seen.add(content)
        review_id = review.get("review_id") or f"{res_id}_APIFY_{index}"
        photo_urls = review.get("review_photos_urls") or []

        converted.append({
            "review_id": review_id,
            "menu_id": menu_id,
            "content": content,
            "photo_url": photo_urls[0] if photo_urls else "",
        })

        if len(converted) >= limit:
            break

    return converted


def _first_value(value):
    if isinstance(value, list) and value:
        return value[0]

    return value


class MenuWiseDB:
    def __init__(self, db_path="menu_wise.db"):
        """데이터베이스 초기화 및 테이블 생성을 위해 만든 생성자입니다.

        커넥션은 스레드별로 따로 둔다(아래 conn 프로퍼티). 비동기 서버가 요청을
        여러 스레드로 처리할 때 각 스레드가 독립 커넥션을 사용하므로, WAL 모드와
        함께 동시 읽기가 실제로 병렬 처리된다(공유 단일 커넥션 병목 제거).
        """
        self.db_path = db_path
        self._local = threading.local()
        self.init_tables()

    def _new_connection(self):
        """스레드 1개가 사용할 SQLite 커넥션을 만들고 동시성 옵션(WAL 등)을 건다.

        WAL: 읽기 여러 개 + 쓰기 1개 동시 처리.
        busy_timeout: 락 충돌 시 즉시 에러 대신 일정 시간 대기·재시도.
        """
        conn = sqlite3.connect(self.db_path, check_same_thread=False)
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA busy_timeout=5000;")
        conn.execute("PRAGMA synchronous=NORMAL;")
        return conn

    @property
    def conn(self):
        """현재 스레드 전용 커넥션을 반환한다(없으면 생성).

        기존 메서드들은 모두 self.conn을 그대로 사용하므로, 코드 수정 없이
        '요청(스레드)별 커넥션'의 이점을 그대로 받는다.
        """
        conn = getattr(self._local, "conn", None)
        if conn is None:
            conn = self._new_connection()
            self._local.conn = conn
        return conn

    def init_tables(self):
        """식당, 메뉴, 리뷰, 그리고 계층적 요약(CoreInfo) 테이블을 생성합니다."""
        cursor = self.conn.cursor()

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS restaurants (
                res_id TEXT PRIMARY KEY,
                res_name TEXT NOT NULL,
                lat REAL,
                lng REAL,
                category TEXT
            )
        """)

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS menus (
                menu_id TEXT PRIMARY KEY,
                res_id TEXT NOT NULL,
                menu_name TEXT NOT NULL,
                price INTEGER,
                photo_url TEXT,
                FOREIGN KEY (res_id) REFERENCES restaurants(res_id)
            )
        """)

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS reviews (
                review_id TEXT PRIMARY KEY,
                res_id TEXT NOT NULL,
                menu_id TEXT,
                content TEXT NOT NULL,
                photo_url TEXT,
                FOREIGN KEY (res_id) REFERENCES restaurants(res_id),
                FOREIGN KEY (menu_id) REFERENCES menus(menu_id)
            )
        """)

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS core_info (
                info_id INTEGER PRIMARY KEY AUTOINCREMENT,
                menu_id TEXT NOT NULL,
                content TEXT NOT NULL,
                info_type TEXT NOT NULL,
                level INTEGER NOT NULL,
                upvotes INTEGER DEFAULT 0,
                downvotes INTEGER DEFAULT 0,
                FOREIGN KEY (menu_id) REFERENCES menus(menu_id)
            )
        """)

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS restaurant_photos (
                res_id TEXT NOT NULL,
                photo_url TEXT NOT NULL,
                PRIMARY KEY (res_id, photo_url),
                FOREIGN KEY (res_id) REFERENCES restaurants(res_id)
            )
        """)

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS comments (
                comment_id INTEGER PRIMARY KEY AUTOINCREMENT,
                info_id INTEGER NOT NULL,
                content TEXT NOT NULL,
                author_token TEXT,
                created_at TEXT DEFAULT (datetime('now')),
                FOREIGN KEY (info_id) REFERENCES core_info(info_id)
            )
        """)

        # 구버전 DB 호환: comments에 author_token 컬럼이 없으면 추가한다(작성자 식별용).
        cursor.execute("PRAGMA table_info(comments)")
        comment_columns = {row[1] for row in cursor.fetchall()}
        if "author_token" not in comment_columns:
            cursor.execute("ALTER TABLE comments ADD COLUMN author_token TEXT")

        self.conn.commit()
    def clean_review_text(self, text):
        """리뷰 원문에서 불필요한 공백과 특수문자를 정리합니다."""
        if text is None:
            return ""

        text = text.strip()
        text = re.sub(r"\s+", " ", text)
        text = re.sub(r"[^가-힣a-zA-Z0-9\s.,!?]", "", text)

        return text
    
    def validate_restaurant_data(self, restaurant):
        """식당 데이터에 필수 값이 있는지 검증합니다."""
        required_fields = ["res_id", "res_name"]

        for field in required_fields:
            if not restaurant.get(field):
                return False

        return True

    def validate_review_data(self, review):
        """리뷰 데이터에 필수 값이 있는지 검증합니다."""
        required_fields = ["review_id", "content"]

        for field in required_fields:
            if not review.get(field):
                return False

        return True
    
    def transform_ai_core_info(self, menu_id, ai_result):
        """AI 요약 JSON을 core_info 테이블 저장 형식으로 변환합니다."""
        transformed = []

        level_1 = ai_result.get("level_1", {})
        pros = level_1.get("pros")
        cons = level_1.get("cons")

        if pros and not is_filler_core_info(pros):
            transformed.append({
                "menu_id": menu_id,
                "content": pros,
                "info_type": "PROS",
                "level": 1,
                "upvotes": 0,
                "downvotes": 0
            })

        if cons and not is_filler_core_info(cons):
            transformed.append({
                "menu_id": menu_id,
                "content": cons,
                "info_type": "CONS",
                "level": 1,
                "upvotes": 0,
                "downvotes": 0
            })

        for item in ai_result.get("level_2", []):
            content = item.get("content", "")
            if not content or is_filler_core_info(content):
                continue
            transformed.append({
                "menu_id": menu_id,
                "content": content,
                "info_type": item.get("info_type", item.get("type", "PROS")),
                "level": 2,
                "upvotes": item.get("upvotes", 0),
                "downvotes": item.get("downvotes", 0)
            })

        return transformed

    def save_restaurant_raw(self, restaurant, reviews, photo_urls=None):
        """[1단계] AI 가공 전 raw 데이터(식당/리뷰/사진 후보)만 DB에 저장한다.

        리뷰는 아직 메뉴에 매칭되지 않았으므로 menu_id는 NULL로 둔다.
        """
        if not self.validate_restaurant_data(restaurant):
            print("유효하지 않은 식당 데이터입니다. 저장을 건너뜁니다.")
            return

        cursor = self.conn.cursor()
        cursor.execute("""
            INSERT OR REPLACE INTO restaurants (res_id, res_name, lat, lng, category)
            VALUES (?, ?, ?, ?, ?)
        """, (
            restaurant["res_id"],
            restaurant["res_name"],
            restaurant.get("lat"),
            restaurant.get("lng"),
            restaurant.get("category"),
        ))

        for review in reviews or []:
            if not self.validate_review_data(review):
                continue

            content = re.sub(r"\s+", " ", str(review.get("content") or "")).strip()
            if len(content) < 5:
                continue

            cursor.execute("""
                INSERT OR IGNORE INTO reviews (review_id, res_id, menu_id, content, photo_url)
                VALUES (?, ?, NULL, ?, ?)
            """, (
                review["review_id"],
                restaurant["res_id"],
                content,
                review.get("photo_url") or "",
            ))

        self.conn.commit()
        self.save_restaurant_photos(restaurant["res_id"], photo_urls or [])

    def save_restaurant_photos(self, res_id, photo_urls):
        """식당 단위 사진 후보 URL을 저장한다(중복은 무시)."""
        cursor = self.conn.cursor()

        for url in photo_urls or []:
            if not url:
                continue

            cursor.execute(
                "INSERT OR IGNORE INTO restaurant_photos (res_id, photo_url) VALUES (?, ?)",
                (res_id, url),
            )

        self.conn.commit()

    def get_restaurant_photos(self, res_id):
        """식당의 사진 후보 URL 목록을 반환한다."""
        cursor = self.conn.cursor()
        cursor.execute(
            "SELECT photo_url FROM restaurant_photos WHERE res_id = ?",
            (res_id,),
        )
        return [row[0] for row in cursor.fetchall() if row[0]]

    def get_all_restaurants(self):
        """저장된 모든 식당을 반환한다(2단계 AI 보강의 대상 목록)."""
        cursor = self.conn.cursor()
        cursor.execute(
            "SELECT res_id, res_name, lat, lng, category FROM restaurants ORDER BY res_id"
        )
        return [
            {
                "res_id": row[0],
                "res_name": row[1],
                "lat": row[2],
                "lng": row[3],
                "category": row[4],
            }
            for row in cursor.fetchall()
        ]

    def get_reviews_by_restaurant(self, res_id):
        """식당의 raw 리뷰 목록을 반환한다."""
        cursor = self.conn.cursor()
        cursor.execute(
            """
            SELECT review_id, content, photo_url
            FROM reviews
            WHERE res_id = ?
            ORDER BY review_id ASC
            """,
            (res_id,),
        )
        return [
            {"review_id": row[0], "content": row[1], "photo_url": row[2] or ""}
            for row in cursor.fetchall()
        ]

    def clear_ai_enrichment(self, res_id):
        """[2단계 재실행 대비] 해당 식당의 AI 산출물(메뉴/핵심정보)을 정리하고
        리뷰의 메뉴 매칭(menu_id)을 초기화한다. raw 리뷰/사진은 그대로 유지된다."""
        cursor = self.conn.cursor()

        cursor.execute("SELECT menu_id FROM menus WHERE res_id = ?", (res_id,))
        menu_ids = [row[0] for row in cursor.fetchall()]

        for menu_id in menu_ids:
            cursor.execute("DELETE FROM core_info WHERE menu_id = ?", (menu_id,))

        cursor.execute("UPDATE reviews SET menu_id = NULL WHERE res_id = ?", (res_id,))
        cursor.execute("DELETE FROM menus WHERE res_id = ?", (res_id,))

        self.conn.commit()

    def assign_reviews_to_menu(self, review_ids, menu_id):
        """raw 리뷰를 대표 매칭 메뉴에 연결한다(menu_id UPDATE)."""
        cursor = self.conn.cursor()

        for review_id in review_ids:
            cursor.execute(
                "UPDATE reviews SET menu_id = ? WHERE review_id = ?",
                (menu_id, review_id),
            )

        self.conn.commit()

    def save_restaurant_data(self, res_data):
        """크롤링한 식당, 메뉴, 리뷰, 핵심 요약 정보를 저장합니다."""
        cursor = self.conn.cursor()

        restaurant = res_data["restaurant"]
        if not self.validate_restaurant_data(restaurant):
            print("유효하지 않은 식당 데이터입니다. 저장을 건너뜁니다.")
            return
        menus = res_data.get("menus", [])
        reviews = res_data.get("reviews", [])
        core_infos = res_data.get("core_info", [])
        if isinstance(core_infos, dict):
            converted_core_infos = []

            for menu_id, ai_result in core_infos.items():
                converted_core_infos.extend(
                    self.transform_ai_core_info(menu_id, ai_result)
                )

            core_infos = converted_core_infos
        cursor.execute("""
            INSERT OR REPLACE INTO restaurants (res_id, res_name, lat, lng, category)
            VALUES (?, ?, ?, ?, ?)
        """, (
            restaurant["res_id"],
            restaurant["res_name"],
            restaurant.get("lat"),
            restaurant.get("lng"),
            restaurant.get("category")
        ))

        for menu in menus:
            cursor.execute("""
                INSERT OR REPLACE INTO menus (menu_id, res_id, menu_name, price, photo_url)
                VALUES (?, ?, ?, ?, ?)
            """, (
                menu["menu_id"],
                restaurant["res_id"],
                menu["menu_name"],
                menu.get("price"),
                menu.get("photo_url")
            ))

        for review in reviews:
            if not self.validate_review_data(review):
                continue
            
            cleaned_content = self.clean_review_text(review.get("content"))

            if len(cleaned_content) < 5:
                continue

            cursor.execute(
                "SELECT review_id FROM reviews WHERE review_id = ?",
                (review["review_id"],)
            )
            existing_review = cursor.fetchone()

            if existing_review:
                continue

            cursor.execute("""
                INSERT INTO reviews (
                    review_id,
                    res_id,
                    menu_id,
                    content,
                    photo_url
                )
                VALUES (?, ?, ?, ?, ?)
            """, (
                review["review_id"],
                restaurant["res_id"],
                review.get("menu_id"),
                cleaned_content,
                review.get("photo_url")
            ))

        for info in core_infos:
            cursor.execute("""
                SELECT info_id FROM core_info
                WHERE menu_id = ?
                AND content = ?
                AND info_type = ?
                AND level = ?
            """, (
                info["menu_id"],
                info["content"],
                info["info_type"],
                info["level"]
            ))

            existing_info = cursor.fetchone()

            if existing_info:
                continue

            cursor.execute("""
                INSERT INTO core_info (menu_id, content, info_type, level, upvotes, downvotes)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (
                info["menu_id"],
                info["content"],
                info["info_type"],
                info["level"],
                info.get("upvotes", 0),
                info.get("downvotes", 0)
            ))

        self.conn.commit()

    def get_nearby_restaurants(self, lat, lng, radius_km):
        """사용자의 위치와 설정된 반경을 바탕으로 식당을 검색합니다."""
        cursor = self.conn.cursor()
        cursor.execute("SELECT res_id, res_name, lat, lng, category FROM restaurants")
        rows = cursor.fetchall()

        nearby = []
        for row in rows:
            res_id, res_name, res_lat, res_lng, category = row
            dist = self._calculate_distance(lat, lng, res_lat, res_lng)

            if dist <= radius_km:
                nearby.append({
                    "res_id": res_id,
                    "res_name": res_name,
                    "category": category,
                    "distance_km": round(dist, 2)
                })

        nearby.sort(key=lambda x: x["distance_km"])
        return nearby
    
    def _representative_core(self, menu_id):
        """대표 장점/단점: (추천 - 비추천)이 가장 높은 코어 정보를 PROS/CONS별로 1개씩 반환."""
        cursor = self.conn.cursor()
        cursor.execute("""
            SELECT content, info_type
            FROM core_info
            WHERE menu_id = ?
            ORDER BY (upvotes - downvotes) DESC, level ASC, info_id ASC
        """, (menu_id,))

        core_pros = ""
        core_cons = ""
        for content, info_type in cursor.fetchall():
            if info_type == "PROS" and not core_pros:
                core_pros = content
            elif info_type == "CONS" and not core_cons:
                core_cons = content
            if core_pros and core_cons:
                break

        return core_pros, core_cons

    def _build_menu_result(self, row, lat=None, lng=None):
        """검색/식당별 메뉴 조회 공통 결과 dict를 만든다."""
        res_id, res_name, res_lat, res_lng, category, menu_id, menu_name, price, photo_url = row

        distance_km = 0.0
        if lat is not None and lng is not None:
            distance_km = self._calculate_distance(lat, lng, res_lat, res_lng)

        core_pros, core_cons = self._representative_core(menu_id)

        return {
            "menu_id": menu_id or "",
            "res_id": res_id or "",
            "restaurant_name": res_name or "",
            "menu_name": menu_name or "",
            "price": price or 0,
            "photo_url": photo_url or "",
            "core_pros": core_pros or "",
            "core_cons": core_cons or "",
            "distance_km": round(distance_km, 2),
            "lat": res_lat or 0.0,
            "lng": res_lng or 0.0,
            "category": category or "",
        }

    def search_menus(self, keyword="", lat=None, lng=None, radius_km=3, keywords=None, search_mode=None):
        """검색 모드(음식점/메뉴/키워드)에 따라 메뉴 검색 결과를 반환한다.

        - 음식점: 식당명/카테고리 매칭
        - 메뉴: 메뉴명만 매칭 (장단점 텍스트는 매칭에서 제외)
        - 키워드(또는 기본): 메뉴명/식당명/카테고리 매칭
        keywords(맛 키워드)가 있으면 메뉴명+장단점에서 추가로 걸러낸다.
        """
        cursor = self.conn.cursor()

        keyword = (keyword or "").strip()
        like = f"%{keyword}%"
        mode = (search_mode or "").strip()

        base_sql = """
            SELECT
                r.res_id, r.res_name, r.lat, r.lng, r.category,
                m.menu_id, m.menu_name, m.price, m.photo_url
            FROM restaurants r
            JOIN menus m ON r.res_id = m.res_id
        """

        params = []
        if keyword:
            if mode == "음식점":
                base_sql += " WHERE (r.res_name LIKE ? OR r.category LIKE ?)"
                params = [like, like]
            elif mode == "메뉴":
                base_sql += " WHERE m.menu_name LIKE ?"
                params = [like]
            else:
                base_sql += " WHERE (m.menu_name LIKE ? OR r.res_name LIKE ? OR r.category LIKE ?)"
                params = [like, like, like]

        cursor.execute(base_sql, params)
        rows = cursor.fetchall()

        results = []
        for row in rows:
            item = self._build_menu_result(row, lat, lng)

            if lat is not None and lng is not None and item["distance_km"] > radius_km:
                continue

            if keywords:
                searchable = f"{item['menu_name']} {item['core_pros']} {item['core_cons']}".lower()
                if not any(str(k).lower() in searchable for k in keywords):
                    continue

            results.append(item)

        results.sort(key=lambda x: x["distance_km"])
        return results

    def get_restaurants_with_menus(self, lat=None, lng=None, radius_km=None, keyword=None):
        """메뉴가 있는 모든 식당을 반환한다(지도 핀용).

        keyword가 있으면 식당명/카테고리뿐 아니라 그 식당의 메뉴명에 검색어가
        포함돼도 노출한다(예: '탕수육'으로 검색 시 탕수육을 파는 식당도 표시).
        """
        cursor = self.conn.cursor()
        cursor.execute("""
            SELECT r.res_id, r.res_name, r.lat, r.lng, r.category,
                   COUNT(m.menu_id) AS menu_count,
                   GROUP_CONCAT(m.menu_name, ' ') AS menu_names
            FROM restaurants r
            JOIN menus m ON r.res_id = m.res_id
            GROUP BY r.res_id
        """)
        rows = cursor.fetchall()

        keyword = (keyword or "").strip().lower()
        results = []
        for res_id, res_name, res_lat, res_lng, category, menu_count, menu_names in rows:
            distance_km = 0.0
            if lat is not None and lng is not None:
                distance_km = self._calculate_distance(lat, lng, res_lat, res_lng)
                if radius_km is not None and distance_km > radius_km:
                    continue

            if keyword:
                # 식당명·카테고리 + 메뉴명까지 검색 대상에 포함
                haystack = f"{res_name or ''} {category or ''} {menu_names or ''}".lower()
                if keyword not in haystack:
                    continue

            results.append({
                "res_id": res_id or "",
                "res_name": res_name or "",
                "lat": res_lat or 0.0,
                "lng": res_lng or 0.0,
                "category": category or "",
                "menu_count": menu_count,
                "distance_km": round(distance_km, 2),
            })

        results.sort(key=lambda x: x["distance_km"])
        return results

    def get_menus_by_restaurant(self, res_id, lat=None, lng=None):
        """특정 식당의 메뉴 목록을 검색 결과와 동일한 형식으로 반환한다."""
        cursor = self.conn.cursor()
        cursor.execute("""
            SELECT
                r.res_id, r.res_name, r.lat, r.lng, r.category,
                m.menu_id, m.menu_name, m.price, m.photo_url
            FROM menus m
            JOIN restaurants r ON m.res_id = r.res_id
            WHERE m.res_id = ?
        """, (res_id,))
        rows = cursor.fetchall()

        return [self._build_menu_result(row, lat, lng) for row in rows]

    def apply_vote(self, info_id, previous, current):
        """추천/비추천 토글 반영. previous/current는 'up'|'down'|'none'.

        이전 표를 취소(감소)하고 새 표를 반영(증가)해 같은 버튼을 두 번 누르면 취소된다.
        """
        cursor = self.conn.cursor()
        cursor.execute(
            "SELECT upvotes, downvotes FROM core_info WHERE info_id = ?",
            (info_id,),
        )
        row = cursor.fetchone()
        if row is None:
            return None

        upvotes, downvotes = row[0], row[1]

        if previous == "up":
            upvotes = max(0, upvotes - 1)
        elif previous == "down":
            downvotes = max(0, downvotes - 1)

        if current == "up":
            upvotes += 1
        elif current == "down":
            downvotes += 1

        cursor.execute(
            "UPDATE core_info SET upvotes = ?, downvotes = ? WHERE info_id = ?",
            (upvotes, downvotes, info_id),
        )
        self.conn.commit()

        return {"info_id": info_id, "upvotes": upvotes, "downvotes": downvotes}

    def add_comment(self, info_id, content, author_token=None):
        """장단점(core_info)에 댓글을 추가하고 저장된 댓글을 반환한다.

        author_token은 작성자(기기)를 식별해 본인 댓글만 수정/삭제할 수 있게 한다.
        """
        content = str(content or "").strip()
        if not content:
            return None

        author_token = (str(author_token).strip() or None) if author_token else None

        cursor = self.conn.cursor()
        cursor.execute(
            "SELECT 1 FROM core_info WHERE info_id = ?",
            (info_id,),
        )
        if cursor.fetchone() is None:
            return None

        cursor.execute(
            "INSERT INTO comments (info_id, content, author_token) VALUES (?, ?, ?)",
            (info_id, content, author_token),
        )
        self.conn.commit()

        comment_id = cursor.lastrowid
        cursor.execute(
            "SELECT comment_id, info_id, content, created_at FROM comments WHERE comment_id = ?",
            (comment_id,),
        )
        row = cursor.fetchone()
        return {
            "comment_id": row[0],
            "info_id": row[1],
            "content": row[2],
            "created_at": row[3],
            "is_mine": True,
        }

    def get_comments(self, info_id, viewer_token=None):
        """장단점(core_info)에 달린 댓글 목록을 오래된 순으로 반환한다.

        viewer_token이 작성자 토큰과 일치하는 댓글은 is_mine=True로 표시해
        프론트에서 본인 댓글에만 수정/삭제 버튼을 노출할 수 있게 한다.
        """
        viewer_token = str(viewer_token).strip() if viewer_token else ""

        cursor = self.conn.cursor()
        cursor.execute(
            """
            SELECT comment_id, info_id, content, created_at, author_token
            FROM comments
            WHERE info_id = ?
            ORDER BY comment_id ASC
            """,
            (info_id,),
        )
        return [
            {
                "comment_id": row[0],
                "info_id": row[1],
                "content": row[2],
                "created_at": row[3],
                "is_mine": bool(viewer_token) and row[4] == viewer_token,
            }
            for row in cursor.fetchall()
        ]

    def update_comment(self, comment_id, content, author_token):
        """본인(author_token 일치) 댓글의 내용을 수정한다. 권한이 없으면 None을 반환한다."""
        content = str(content or "").strip()
        author_token = str(author_token).strip() if author_token else ""
        if not content or not author_token:
            return None

        cursor = self.conn.cursor()
        cursor.execute(
            "SELECT author_token FROM comments WHERE comment_id = ?",
            (comment_id,),
        )
        row = cursor.fetchone()
        if row is None or row[0] != author_token:
            return None

        cursor.execute(
            "UPDATE comments SET content = ? WHERE comment_id = ?",
            (content, comment_id),
        )
        self.conn.commit()

        cursor.execute(
            "SELECT comment_id, info_id, content, created_at FROM comments WHERE comment_id = ?",
            (comment_id,),
        )
        updated = cursor.fetchone()
        return {
            "comment_id": updated[0],
            "info_id": updated[1],
            "content": updated[2],
            "created_at": updated[3],
            "is_mine": True,
        }

    def delete_comment(self, comment_id, author_token):
        """본인(author_token 일치) 댓글을 삭제한다. 권한이 없으면 False를 반환한다."""
        author_token = str(author_token).strip() if author_token else ""
        if not author_token:
            return False

        cursor = self.conn.cursor()
        cursor.execute(
            "SELECT author_token FROM comments WHERE comment_id = ?",
            (comment_id,),
        )
        row = cursor.fetchone()
        if row is None or row[0] != author_token:
            return False

        cursor.execute("DELETE FROM comments WHERE comment_id = ?", (comment_id,))
        self.conn.commit()
        return True

    def delete_menus_by_names(self, names):
        """곁들임/밑반찬 등 제외 대상 메뉴를 삭제한다(연결된 core_info 삭제, 리뷰는 menu_id 해제)."""
        cursor = self.conn.cursor()
        deleted = 0
        for name in names:
            cursor.execute("SELECT menu_id FROM menus WHERE menu_name = ?", (name,))
            menu_ids = [r[0] for r in cursor.fetchall()]
            for menu_id in menu_ids:
                cursor.execute("DELETE FROM core_info WHERE menu_id = ?", (menu_id,))
                cursor.execute("UPDATE reviews SET menu_id = NULL WHERE menu_id = ?", (menu_id,))
                cursor.execute("DELETE FROM menus WHERE menu_id = ?", (menu_id,))
                deleted += 1
        self.conn.commit()
        return deleted

    def vote(self, info_id, is_upvote):
        """(구버전 호환) 단순 증가."""
        self.update_feedback(info_id, is_upvote)

        cursor = self.conn.cursor()
        cursor.execute("""
            SELECT info_id, upvotes, downvotes
            FROM core_info
            WHERE info_id = ?
        """, (info_id,))

        row = cursor.fetchone()

        if row is None:
            return None

        return {
            "info_id": row[0],
            "upvotes": row[1],
            "downvotes": row[2]
        }

    def get_menu_details(self, menu_id):
        """메뉴 상세 정보와 core_info 목록을 백엔드 응답 구조에 맞게 반환합니다."""
        cursor = self.conn.cursor()

        cursor.execute("""
            SELECT
                m.menu_id,
                m.menu_name,
                m.price,
                m.photo_url,
                r.res_id,
                r.res_name,
                r.category,
                r.lat,
                r.lng
            FROM menus m
            JOIN restaurants r ON m.res_id = r.res_id
            WHERE m.menu_id = ?
        """, (menu_id,))

        menu_row = cursor.fetchone()

        if menu_row is None:
            return None

        cursor.execute("""
            SELECT info_id, menu_id, content, info_type, level, upvotes, downvotes,
                   (SELECT COUNT(*) FROM comments c WHERE c.info_id = core_info.info_id) AS comment_count
            FROM core_info
            WHERE menu_id = ?
            ORDER BY (upvotes - downvotes) DESC, level ASC, info_id ASC
        """, (menu_id,))

        core_rows = cursor.fetchall()

        details = []
        for row in core_rows:
            details.append({
                "info_id": row[0],
                "menu_id": row[1],
                "content": row[2],
                "info_type": row[3],
                "level": row[4],
                "upvotes": row[5],
                "downvotes": row[6],
                "comment_count": row[7]
            })

        return {
            "menu_id": menu_row[0] or "",
            "menu_name": menu_row[1] or "",
            "price": menu_row[2] or 0,
            "photo_url": menu_row[3] or "",
            "restaurant": {
                "res_id": menu_row[4] or "",
                "res_name": menu_row[5] or "",
                "category": menu_row[6] or "",
                "lat": menu_row[7] or 0.0,
                "lng": menu_row[8] or 0.0
            },
            "details": details,
            "core_info": details
        }
    def get_restaurants_by_category(self, category):
        """카테고리 기준으로 식당을 조회합니다."""
        cursor = self.conn.cursor()
        cursor.execute("""
            SELECT res_id, res_name, lat, lng, category
            FROM restaurants
            WHERE category = ?
        """, (category,))

        rows = cursor.fetchall()

        restaurants = []
        for row in rows:
            res_id, res_name, lat, lng, category = row
            restaurants.append({
                "res_id": res_id,
                "res_name": res_name,
                "lat": lat,
                "lng": lng,
                "category": category
            })

        return restaurants
    
    def get_top_restaurants_by_upvotes(self):
        """추천 수 기준 인기 식당 조회"""
        cursor = self.conn.cursor()

        cursor.execute("""
            SELECT
                restaurants.res_id,
                restaurants.res_name,
                restaurants.category,
                SUM(core_info.upvotes) as total_upvotes
            FROM restaurants
            JOIN menus ON restaurants.res_id = menus.res_id
            JOIN core_info ON menus.menu_id = core_info.menu_id
            GROUP BY restaurants.res_id
            ORDER BY total_upvotes DESC
        """)

        rows = cursor.fetchall()

        result = []

        for row in rows:
            result.append({
                "res_id": row[0],
                "res_name": row[1],
                "category": row[2],
                "total_upvotes": row[3]
            })

        return result

    def get_review_count(self, res_id):
        """특정 식당의 리뷰 개수를 조회합니다."""
        cursor = self.conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM reviews WHERE res_id = ?", (res_id,))
        return cursor.fetchone()[0]
    def get_menu_review_count(self, menu_id):
        """특정 메뉴의 리뷰 개수를 조회합니다."""
        cursor = self.conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM reviews WHERE menu_id = ?", (menu_id,))
        return cursor.fetchone()[0]

    def update_feedback(self, info_id, is_upvote):
        """사용자가 누른 추천/비추천 수치를 DB 컬럼에 실시간 반영합니다."""
        cursor = self.conn.cursor()
        column = "upvotes" if is_upvote else "downvotes"

        cursor.execute(
            f"UPDATE core_info SET {column} = {column} + 1 WHERE info_id = ?",
            (info_id,)
        )

        self.conn.commit()

    def _calculate_distance(self, lat1, lon1, lat2, lon2):
        """Haversine 공식을 이용한 거리 계산"""
        if None in (lat1, lon1, lat2, lon2):
            return float("inf")

        r = 6371
        dlat = math.radians(lat2 - lat1)
        dlon = math.radians(lon2 - lon1)

        a = (
            math.sin(dlat / 2) ** 2
            + math.cos(math.radians(lat1))
            * math.cos(math.radians(lat2))
            * math.sin(dlon / 2) ** 2
        )

        c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
        return r * c

    def close(self):
        conn = getattr(self._local, "conn", None)
        if conn is not None:
            conn.close()
            self._local.conn = None


class ReviewCrawler:
    """공식/공개 API 기반 음식점 수집기.

    GOOGLE_PLACES_API_KEY가 있으면 Google Places API로 식당, 사진, 일부 리뷰를 조회합니다.
    Google 키가 없으면 KAKAO_REST_API_KEY로 카카오 Local API를 사용합니다.
    지도 API는 메뉴명/가격을 안정적으로 제공하지 않으므로 대표 메뉴 placeholder를 생성합니다.
    """

    KANGWON_UNIV_LAT = 37.8683
    KANGWON_UNIV_LNG = 127.7445

    def __init__(self, center_lat=None, center_lng=None, radius_m=1500):
        self._load_env_files()
        self.center_lat = center_lat or self.KANGWON_UNIV_LAT
        self.center_lng = center_lng or self.KANGWON_UNIV_LNG
        self.radius_m = radius_m
        self.google_api_key = os.getenv("GOOGLE_PLACES_API_KEY")
        self.kakao_api_key = os.getenv("KAKAO_REST_API_KEY")
        self.place_names = {}

    def crawl_restaurant_info(self, keyword="강원대", lat=None, lng=None, radius_m=None, limit=15):
        """춘천 강원대 주변 음식점을 실제 API로 조회합니다.

        카카오 Local API 키가 없으면 기존 개발 흐름을 위해 mock 데이터를 반환합니다.
        """
        lat = lat if lat is not None else self.center_lat
        lng = lng if lng is not None else self.center_lng
        radius_m = radius_m or self.radius_m

        if self.google_api_key:
            return self._crawl_google_restaurant_info_max(keyword, lat, lng, radius_m, limit)

        if not self.kakao_api_key:
            return self._mock_restaurant_info(keyword, lat, lng)

        documents = self._search_kakao_restaurants(keyword, lat, lng, radius_m, limit)
        restaurants = []

        for place in documents:
            res_id = f"KAKAO_{place.get('id')}"
            res_name = place.get("place_name") or "이름 없는 식당"
            category = self._last_category(place.get("category_name", ""))
            menu_id = f"{res_id}_MENU"
            self.place_names[res_id] = res_name

            restaurants.append({
                "restaurant": {
                    "res_id": res_id,
                    "res_name": res_name,
                    "lat": self._safe_float(place.get("y")),
                    "lng": self._safe_float(place.get("x")),
                    "category": category or "음식점",
                },
                "menus": [
                    {
                        "menu_id": menu_id,
                        "menu_name": f"{res_name} 대표 메뉴",
                        "price": 0,
                        "photo_url": "",
                    }
                ],
                "source": {
                    "provider": "kakao_local",
                    "place_url": place.get("place_url", ""),
                    "address": place.get("road_address_name") or place.get("address_name", ""),
                    "phone": place.get("phone", ""),
                },
            })

        return restaurants

    def crawl_reviews_with_retry(self, res_id, max_retries=3, delay=1):
        """리뷰 크롤링 실패 시 일정 횟수 재시도합니다."""
        for attempt in range(1, max_retries + 1):
            try:
                reviews = self.crawl_reviews(res_id)

                if reviews:
                    return reviews

                print(f"리뷰 크롤링 결과 없음 - 재시도 {attempt}/{max_retries}")

            except Exception as e:
                print(f"리뷰 크롤링 실패 - 재시도 {attempt}/{max_retries}: {e}")

            time.sleep(delay)

        return []

    def crawl_reviews(self, res_id):
        """Google Places API로 일부 장소 리뷰를 조회합니다.

        Kakao Local API는 리뷰 본문을 제공하지 않으므로 Kakao 장소는 빈 리스트를 반환합니다.
        """
        if res_id == "R001":
            return self._mock_reviews()

        if not res_id.startswith("GOOGLE_") or not self.google_api_key:
            return []

        place_id = res_id.replace("GOOGLE_", "", 1)
        return self._crawl_google_reviews_max(place_id)

    def _crawl_google_restaurant_info_max(self, keyword, lat, lng, radius_m, limit):
        results = []
        seen_place_ids = set()
        next_page_token = None

        while len(results) < limit:
            params = {
                "location": f"{lat},{lng}",
                "radius": radius_m,
                "type": "restaurant",
                "language": "ko",
                "key": self.google_api_key,
            }

            if next_page_token:
                time.sleep(2)
                params = {
                    "pagetoken": next_page_token,
                    "key": self.google_api_key,
                }

            data = self._request_json(
                "https://maps.googleapis.com/maps/api/place/nearbysearch/json",
                params=params,
            )

            for place in data.get("results", []):
                place_id = place.get("place_id")
                if not place_id or place_id in seen_place_ids:
                    continue

                seen_place_ids.add(place_id)
                results.append(place)

                if len(results) >= limit:
                    break

            next_page_token = data.get("next_page_token")
            if not next_page_token:
                break

        results.sort(
            key=lambda item: item.get("user_ratings_total", 0),
            reverse=True,
        )

        restaurants = []
        for place in results[:limit]:
            place_id = place.get("place_id")
            if not place_id:
                continue

            detail = self._google_place_details(
                place_id,
                fields="photos,formatted_phone_number,user_ratings_total,rating",
            )
            detail_result = detail.get("result", {})
            res_id = f"GOOGLE_{place_id}"
            res_name = place.get("name") or "이름 없는 식당"
            geometry = place.get("geometry", {}).get("location", {})
            category = self._google_category(place.get("types", []))
            photo_urls = self._google_photo_urls(
                detail_result.get("photos") or place.get("photos", []),
                limit=10,
            )
            menu_id = f"{res_id}_MENU"
            self.place_names[res_id] = res_name

            restaurants.append({
                "restaurant": {
                    "res_id": res_id,
                    "res_name": res_name,
                    "lat": self._safe_float(geometry.get("lat")),
                    "lng": self._safe_float(geometry.get("lng")),
                    "category": category,
                },
                "menus": [
                    {
                        "menu_id": menu_id,
                        "menu_name": f"{res_name} 대표 메뉴",
                        "price": 0,
                        "photo_url": photo_urls[0] if photo_urls else "",
                    }
                ],
                "source": {
                    "provider": "google_places",
                    "place_url": f"https://www.google.com/maps/place/?q=place_id:{place_id}",
                    "address": place.get("formatted_address", ""),
                    "phone": detail_result.get("formatted_phone_number", ""),
                    "photo_urls": photo_urls,
                    "rating": detail_result.get("rating") or place.get("rating", 0),
                    "user_ratings_total": (
                        detail_result.get("user_ratings_total")
                        or place.get("user_ratings_total", 0)
                    ),
                },
            })

        return restaurants

    def _crawl_google_restaurant_info(self, keyword, lat, lng, radius_m, limit):
        data = self._request_json(
            "https://maps.googleapis.com/maps/api/place/textsearch/json",
            params={
                "query": f"{keyword} 음식점",
                "location": f"{lat},{lng}",
                "radius": radius_m,
                "language": "ko",
                "key": self.google_api_key,
            },
        )

        restaurants = []
        for place in data.get("results", [])[:limit]:
            place_id = place.get("place_id")
            if not place_id:
                continue

            detail = self._google_place_details(
                place_id,
                fields="photos,formatted_phone_number",
            )
            detail_result = detail.get("result", {})
            res_id = f"GOOGLE_{place_id}"
            res_name = place.get("name") or "이름 없는 식당"
            geometry = place.get("geometry", {}).get("location", {})
            category = self._google_category(place.get("types", []))
            photo_urls = self._google_photo_urls(
                detail_result.get("photos") or place.get("photos", []),
                limit=5,
            )
            menu_id = f"{res_id}_MENU"
            self.place_names[res_id] = res_name

            restaurants.append({
                "restaurant": {
                    "res_id": res_id,
                    "res_name": res_name,
                    "lat": self._safe_float(geometry.get("lat")),
                    "lng": self._safe_float(geometry.get("lng")),
                    "category": category,
                },
                "menus": [
                    {
                        "menu_id": menu_id,
                        "menu_name": f"{res_name} 대표 메뉴",
                        "price": 0,
                        "photo_url": photo_urls[0] if photo_urls else "",
                    }
                ],
                "source": {
                    "provider": "google_places",
                    "place_url": f"https://www.google.com/maps/place/?q=place_id:{place_id}",
                    "address": place.get("formatted_address", ""),
                    "phone": detail_result.get("formatted_phone_number", ""),
                    "photo_urls": photo_urls,
                },
            })

        return restaurants

    def _search_kakao_restaurants(self, keyword, lat, lng, radius_m, limit):
        documents = []
        page = 1

        while len(documents) < limit and page <= 3:
            params = {
                "query": keyword,
                "category_group_code": "FD6",
                "x": lng,
                "y": lat,
                "radius": radius_m,
                "sort": "distance",
                "size": min(15, limit - len(documents)),
                "page": page,
            }
            data = self._request_json(
                "https://dapi.kakao.com/v2/local/search/keyword.json",
                params=params,
                headers={"Authorization": f"KakaoAK {self.kakao_api_key}"},
            )
            documents.extend(data.get("documents", []))

            if data.get("meta", {}).get("is_end", True):
                break

            page += 1

        return documents[:limit]

    def _crawl_google_reviews_max(self, place_id, limit=40):
        reviews = []
        seen_contents = set()
        menu_id = f"GOOGLE_{place_id}_MENU"

        for sort in ("most_relevant", "newest"):
            detail_data = self._google_place_details(
                place_id,
                fields="reviews",
                reviews_sort=sort,
            )

            for item in detail_data.get("result", {}).get("reviews", []):
                content = item.get("text", "").strip()
                if not content or content in seen_contents:
                    continue

                seen_contents.add(content)
                review_hash = hashlib.md5(content.encode("utf-8")).hexdigest()[:12]
                reviews.append({
                    "review_id": f"GOOGLE_{place_id}_{sort}_{review_hash}",
                    "menu_id": menu_id,
                    "content": content,
                    "photo_url": None,
                    "rating": item.get("rating"),
                    "relative_time_description": item.get("relative_time_description", ""),
                })

                if len(reviews) >= limit:
                    return reviews

        return reviews

    def _crawl_google_reviews(self, place_id):
        detail_data = self._google_place_details(place_id, fields="reviews")

        reviews = []
        menu_id = f"GOOGLE_{place_id}_MENU"
        for item in detail_data.get("result", {}).get("reviews", []):
            content = item.get("text", "")
            if not content:
                continue

            review_hash = hashlib.md5(content.encode("utf-8")).hexdigest()[:12]
            reviews.append({
                "review_id": f"GOOGLE_{place_id}_{review_hash}",
                "menu_id": menu_id,
                "content": content,
                "photo_url": None,
            })

        return reviews

    def _google_place_details(self, place_id, fields, reviews_sort=None):
        params = {
            "place_id": place_id,
            "fields": fields,
            "language": "ko",
            "key": self.google_api_key,
        }

        if reviews_sort:
            params["reviews_sort"] = reviews_sort

        return self._request_json(
            "https://maps.googleapis.com/maps/api/place/details/json",
            params=params,
        )

    def _google_photo_urls(self, photos, limit=5):
        if not photos:
            return []

        urls = []
        for photo in photos[:limit]:
            reference = photo.get("photo_reference")
            if not reference:
                continue

            params = urlencode({
                "maxwidth": 800,
                "photo_reference": reference,
                "key": self.google_api_key,
            })
            urls.append(f"https://maps.googleapis.com/maps/api/place/photo?{params}")

        return urls

    def _google_category(self, types):
        category_map = {
            "bakery": "제과,베이커리",
            "bar": "주점",
            "cafe": "카페",
            "meal_delivery": "배달음식",
            "meal_takeaway": "포장음식",
            "restaurant": "음식점",
        }

        for item in types:
            if item in category_map:
                return category_map[item]

        return "음식점"

    def _request_json(self, url, params=None, headers=None, timeout=10):
        params = params or {}
        headers = headers or {}
        full_url = f"{url}?{urlencode(params)}" if params else url
        request = Request(full_url, headers=headers)

        with urlopen(request, timeout=timeout) as response:
            return json.loads(response.read().decode("utf-8"))

    def _load_env_files(self):
        """python-dotenv 없이 루트/크롤러 .env의 API 키를 읽습니다."""
        candidates = [
            os.path.join(os.getcwd(), ".env"),
            os.path.join(os.path.dirname(__file__), ".env"),
            os.path.join(os.path.dirname(__file__), "..", ".env"),
        ]

        for path in candidates:
            path = os.path.abspath(path)
            if not os.path.exists(path):
                continue

            with open(path, encoding="utf-8") as env_file:
                for line in env_file:
                    line = line.strip()
                    if not line or line.startswith("#") or "=" not in line:
                        continue

                    key, value = line.split("=", 1)
                    key = key.strip().lstrip("\ufeff")
                    value = value.strip().strip('"').strip("'")
                    os.environ.setdefault(key, value)

    def _mock_restaurant_info(self, keyword, lat, lng):
        return [
            {
                "restaurant": {
                    "res_id": "R001",
                    "res_name": f"{keyword} 맛집 1호점",
                    "lat": lat,
                    "lng": lng,
                    "category": "한식",
                },
                "menus": [
                    {
                        "menu_id": "M001",
                        "menu_name": "김치찌개",
                        "price": 9000,
                        "photo_url": "https://example.com/kimchi.jpg",
                    },
                    {
                        "menu_id": "M002",
                        "menu_name": "된장찌개",
                        "price": 8500,
                        "photo_url": "https://example.com/doenjang.jpg",
                    },
                ],
            }
        ]

    def _mock_reviews(self):
        return [
            {
                "review_id": "RV001",
                "menu_id": "M001",
                "content": "김치찌개가 얼큰하고 맛있었지만 조금 짰어요.",
                "photo_url": "https://example.com/review1.jpg",
            },
            {
                "review_id": "RV002",
                "menu_id": "M002",
                "content": "된장찌개가 구수하고 가격도 괜찮아요.",
                "photo_url": "https://example.com/review2.jpg",
            },
            {
                "review_id": "RV003",
                "menu_id": "M001",
                "content": "김치찌개가 얼큰해서 좋았는데 조금 짰어요.",
                "photo_url": None,
            },
            {
                "review_id": "RV004",
                "menu_id": "M002",
                "content": "된장찌개 양이 많고 맛도 무난했어요.",
                "photo_url": None,
            },
        ]

    def _last_category(self, category_name):
        if not category_name:
            return ""
        return category_name.split(">")[-1].strip()

    def _safe_float(self, value):
        try:
            return float(value)
        except (TypeError, ValueError):
            return None
