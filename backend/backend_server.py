import os
import sys
from pathlib import Path
from typing import List, Optional

from fastapi import FastAPI, HTTPException, Query
from fastapi.concurrency import run_in_threadpool
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

try:
    from dotenv import load_dotenv
except ModuleNotFoundError:
    def load_dotenv(*args, **kwargs):
        return False


def _load_env_files() -> None:
    current_dir = Path(__file__).resolve().parent
    for candidate in (current_dir / ".env", current_dir.parent / ".env", current_dir.parent.parent / ".env"):
        if candidate.exists():
            load_dotenv(dotenv_path=candidate)
    load_dotenv()


def _add_import_paths() -> None:
    current_dir = Path(__file__).resolve().parent
    for candidate in (current_dir, current_dir.parent, current_dir.parent.parent):
        path = str(candidate)
        if path not in sys.path:
            sys.path.insert(0, path)


_add_import_paths()
_load_env_files()

app = FastAPI(
    title="MenuWise API",
    description="음식점 리뷰 기반 메뉴 추천 시스템 백엔드 API",
    version="0.2.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=os.getenv("CORS_ALLOW_ORIGINS", "*").split(","),
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

USE_DUMMY = os.getenv("USE_DUMMY", "0").lower() in {"1", "true", "yes", "y"}

db = None
db_init_error = None
try:
    from crawler.database_and_crawler import MenuWiseDB

    db = MenuWiseDB(os.getenv("MENUWISE_DB_PATH", str(Path(__file__).resolve().parents[1] / "menu_wise.db")))
except Exception as exc:  # 서버는 띄우되 /health에서 원인을 확인할 수 있게 둔다.
    db_init_error = str(exc)

processor = None
ai_init_error = None
try:
    from ai_engine.ai_analyzer import MenuAIProcessor

    processor = MenuAIProcessor(os.getenv("OPENAI_API_KEY", ""))
except Exception as exc:
    ai_init_error = str(exc)


DUMMY_SEARCH_RESULTS = [
    {
        "menu_id": "menu_001",
        "restaurant_name": "맛있는 국밥집",
        "menu_name": "순대국밥",
        "price": 9000,
        "photo_url": "https://example.com/photo1.jpg",
        "core_pros": "진한 국물, 고기 푸짐",
        "core_cons": "간이 센 편",
        "distance_km": 0.3,
        "lat": 37.8813,
        "lng": 127.7298,
        "category": "한식",
    },
    {
        "menu_id": "menu_002",
        "restaurant_name": "청춘 떡볶이",
        "menu_name": "매운 떡볶이",
        "price": 6000,
        "photo_url": "https://example.com/photo2.jpg",
        "core_pros": "중독성 있는 매운맛",
        "core_cons": "매우 매움 주의",
        "distance_km": 0.7,
        "lat": 37.8821,
        "lng": 127.7310,
        "category": "분식",
    },
]

DUMMY_DETAILS = [
    {
        "info_id": 1,
        "menu_id": "menu_001",
        "content": "국물이 정말 진하고 맛있어요. 고기도 푸짐해서 가성비가 좋습니다.",
        "info_type": "PROS",
        "level": 2,
        "upvotes": 12,
        "downvotes": 1,
    },
    {
        "info_id": 2,
        "menu_id": "menu_001",
        "content": "간이 센 편이라 짜게 느낄 수 있습니다.",
        "info_type": "CONS",
        "level": 2,
        "upvotes": 5,
        "downvotes": 2,
    },
]

DUMMY_SUMMARY = {
    "menu_id": "menu_001",
    "menu_name": "순대국밥",
    "photo_url": "https://example.com/photo1.jpg",
    "level_1": {
        "pros": "진한 국물, 푸짐한 고기, 좋은 가성비",
        "cons": "간이 센 편",
    },
    "level_2": [
        {"content": "국물이 진하고 깊은 맛이 납니다.", "type": "PROS"},
        {"content": "고기 양이 많아 든든합니다.", "type": "PROS"},
        {"content": "짜게 느낄 수 있습니다.", "type": "CONS"},
    ],
}


class VoteRequest(BaseModel):
    info_id: int
    vote: str = "none"      # 'up' | 'down' | 'none' (현재 누른 상태)
    previous: str = "none"  # 'up' | 'down' | 'none' (직전 상태, 취소/전환 계산용)


class CommentRequest(BaseModel):
    content: str
    author_token: Optional[str] = None


class CommentUpdateRequest(BaseModel):
    content: str
    author_token: str


def _require_db():
    if db is None:
        detail = "DB가 초기화되지 않았습니다."
        if db_init_error:
            detail = f"{detail} 원인: {db_init_error}"
        raise HTTPException(status_code=503, detail=detail)
    return db


def _require_ai():
    if processor is None:
        detail = "AI 프로세서가 초기화되지 않았습니다."
        if ai_init_error:
            detail = f"{detail} 원인: {ai_init_error}"
        raise HTTPException(status_code=503, detail=detail)
    return processor


def _split_keywords(keywords: Optional[str]) -> List[str]:
    if not keywords:
        return []
    return [keyword.strip() for keyword in keywords.split(",") if keyword.strip()]


def _fetch_review_texts(menu_id: str) -> List[str]:
    database = _require_db()
    cursor = database.conn.cursor()
    cursor.execute(
        """
        SELECT content
        FROM reviews
        WHERE menu_id = ?
        ORDER BY review_id ASC
        """,
        (menu_id,),
    )
    return [row[0] for row in cursor.fetchall() if row[0]]


def _fetch_photo_urls(menu_id: str) -> List[str]:
    database = _require_db()
    cursor = database.conn.cursor()
    cursor.execute(
        """
        SELECT photo_url
        FROM reviews
        WHERE menu_id = ?
          AND photo_url IS NOT NULL
          AND photo_url != ''
        """,
        (menu_id,),
    )
    return [row[0] for row in cursor.fetchall() if row[0]]


def _fetch_restaurant_photo_urls(res_id: str) -> List[str]:
    """해당 식당의 사진 후보 URL만 반환한다(다른 식당 사진은 섞이지 않는다)."""
    if not res_id:
        return []

    database = _require_db()
    cursor = database.conn.cursor()
    try:
        cursor.execute(
            "SELECT photo_url FROM restaurant_photos WHERE res_id = ?",
            (res_id,),
        )
    except Exception:
        # 구버전 DB에 restaurant_photos 테이블이 없을 수 있다.
        return []
    return [row[0] for row in cursor.fetchall() if row[0]]


def _summary_from_core_info(core_info: List[dict]):
    """저장된 core_info 목록을 프론트 요약 응답(level_1/level_2) 형식으로 변환한다."""
    level_1 = {"pros": "", "cons": ""}
    level_2: List[dict] = []

    for item in core_info:
        content = (item.get("content") or "").strip()
        info_type = item.get("info_type") or "PROS"
        level = item.get("level")

        if not content:
            continue

        if level == 1:
            if info_type == "PROS" and not level_1["pros"]:
                level_1["pros"] = content
            elif info_type == "CONS" and not level_1["cons"]:
                level_1["cons"] = content
        elif level == 2:
            level_2.append({"content": content, "type": info_type})

    return level_1, level_2


def _save_ai_summary(menu_id: str, summary: dict) -> None:
    database = _require_db()
    transformed = database.transform_ai_core_info(menu_id, summary)
    cursor = database.conn.cursor()

    for info in transformed:
        content = (info.get("content") or "").strip()
        info_type = (info.get("info_type") or "PROS").strip()
        level = int(info.get("level", 2))

        if not content:
            continue

        cursor.execute(
            """
            SELECT info_id
            FROM core_info
            WHERE menu_id = ?
              AND content = ?
              AND info_type = ?
              AND level = ?
            """,
            (menu_id, content, info_type, level),
        )
        if cursor.fetchone():
            continue

        cursor.execute(
            """
            INSERT INTO core_info (menu_id, content, info_type, level, upvotes, downvotes)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                menu_id,
                content,
                info_type,
                level,
                int(info.get("upvotes", 0)),
                int(info.get("downvotes", 0)),
            ),
        )

    database.conn.commit()


@app.get("/health", summary="서버 상태 확인")
async def health():
    return {
        "ok": True,
        "use_dummy": USE_DUMMY,
        "db_ready": db is not None,
        "ai_ready": processor is not None,
        "db_error": db_init_error,
        "ai_error": ai_init_error,
    }


@app.get("/api/health", summary="API 상태 확인")
async def api_health():
    return await health()


@app.get(
    "/api/search",
    summary="주변 메뉴 검색",
    description="위치, 반경, 검색어, 맛 키워드 기준으로 메뉴 검색 결과를 반환합니다.",
)
def search_menus(
    lat: float,
    lng: float,
    radius: Optional[float] = Query(default=None),
    radius_km: Optional[float] = Query(default=None),
    query: Optional[str] = "",
    search_mode: Optional[str] = None,
    keywords: Optional[str] = None,
):
    if USE_DUMMY:
        return {"results": DUMMY_SEARCH_RESULTS}

    database = _require_db()
    resolved_radius = radius_km if radius_km is not None else radius
    if resolved_radius is None:
        resolved_radius = 3.0

    try:
        results = database.search_menus(
            keyword=query or "",
            lat=lat,
            lng=lng,
            radius_km=resolved_radius,
            keywords=_split_keywords(keywords),
            search_mode=search_mode,
        )
        results.sort(key=lambda item: item.get("distance_km", float("inf")))
        return {"results": results}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"검색 중 오류 발생: {exc}") from exc


@app.get(
    "/api/menu/{menu_id}/details",
    summary="메뉴 상세 조회",
    description="menu_id에 해당하는 코어 리뷰 목록을 반환합니다.",
)
def get_details(menu_id: str):
    if USE_DUMMY:
        return {"details": DUMMY_DETAILS}

    database = _require_db()
    try:
        menu_data = database.get_menu_details(menu_id)
        if not menu_data:
            raise HTTPException(status_code=404, detail=f"menu_id '{menu_id}'에 해당하는 메뉴가 없습니다.")
        return {"details": menu_data.get("core_info", [])}
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"상세 조회 중 오류 발생: {exc}") from exc


@app.get(
    "/api/menu/{menu_id}/summary",
    summary="AI 메뉴 요약 + 대표 사진 조회",
    description="리뷰 원문을 AI로 요약하고 대표 사진 URL을 반환합니다.",
)
async def get_menu_summary(menu_id: str):
    if USE_DUMMY:
        return DUMMY_SUMMARY

    database = _require_db()

    try:
        menu_data = await run_in_threadpool(database.get_menu_details, menu_id)
        if not menu_data:
            raise HTTPException(status_code=404, detail=f"menu_id '{menu_id}'에 해당하는 메뉴가 없습니다.")

        menu_name = menu_data["menu_name"]
        core_info = menu_data.get("core_info", [])

        # 크롤링 단계에서 만들어 둔 핵심 요약을 우선 사용한다.
        level_1, level_2 = _summary_from_core_info(core_info)

        # 저장된 요약이 전혀 없을 때만 실시간 AI 요약을 수행한다.
        if not level_1["pros"] and not level_1["cons"] and not level_2:
            ai_processor = _require_ai()
            review_texts = _fetch_review_texts(menu_id)
            if not review_texts:
                review_texts = [item["content"] for item in core_info if item.get("content")]

            summary = await ai_processor.analyze_reviews(menu_name, review_texts)
            _save_ai_summary(menu_id, summary)
            level_1 = summary.get("level_1", level_1)
            level_2 = summary.get("level_2", level_2)

        # 메뉴에 매칭된 대표 사진이 없을 때만, 같은 식당 사진 풀에서 다시 매칭한다.
        best_photo = menu_data.get("photo_url") or ""
        if not best_photo and processor is not None:
            res_id = menu_data.get("restaurant", {}).get("res_id", "")
            image_urls = _fetch_photo_urls(menu_id)
            if not image_urls:
                image_urls = _fetch_restaurant_photo_urls(res_id)

            if image_urls:
                matched_photo = await run_in_threadpool(processor.match_photo, menu_name, image_urls)
                if matched_photo and matched_photo != "default_url":
                    best_photo = matched_photo

        return {
            "menu_id": menu_id,
            "menu_name": menu_name,
            "photo_url": best_photo,
            "level_1": level_1,
            "level_2": level_2,
        }
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"AI 요약 중 오류 발생: {exc}") from exc


@app.get(
    "/api/restaurants",
    summary="주변 식당 목록",
    description="메뉴가 있는 주변 식당 전체를 반환합니다(지도 핀용).",
)
def get_restaurants(
    lat: Optional[float] = None,
    lng: Optional[float] = None,
    radius: Optional[float] = Query(default=None),
    radius_km: Optional[float] = Query(default=None),
    query: Optional[str] = None,
):
    if USE_DUMMY:
        seen = {}
        for item in DUMMY_SEARCH_RESULTS:
            seen.setdefault(item["restaurant_name"], {
                "res_id": item["menu_id"],
                "res_name": item["restaurant_name"],
                "lat": item["lat"],
                "lng": item["lng"],
                "category": item["category"],
                "menu_count": 1,
                "distance_km": item["distance_km"],
            })
        return {"restaurants": list(seen.values())}

    database = _require_db()
    resolved_radius = radius_km if radius_km is not None else radius

    try:
        restaurants = database.get_restaurants_with_menus(
            lat=lat,
            lng=lng,
            radius_km=resolved_radius,
            keyword=query,
        )
        return {"restaurants": restaurants}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"식당 조회 중 오류 발생: {exc}") from exc


@app.get(
    "/api/restaurant/{res_id}/menus",
    summary="식당의 메뉴 목록",
    description="해당 식당의 모든 메뉴를 검색 결과와 동일한 형식으로 반환합니다.",
)
def get_restaurant_menus(
    res_id: str,
    lat: Optional[float] = None,
    lng: Optional[float] = None,
):
    if USE_DUMMY:
        return {"results": DUMMY_SEARCH_RESULTS}

    database = _require_db()
    try:
        results = database.get_menus_by_restaurant(res_id, lat=lat, lng=lng)
        if not results:
            raise HTTPException(status_code=404, detail=f"res_id '{res_id}'의 메뉴가 없습니다.")
        return {"results": results}
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"식당 메뉴 조회 중 오류 발생: {exc}") from exc


@app.post(
    "/api/vote",
    summary="리뷰 추천/비추천(토글)",
    description="같은 버튼을 두 번 누르면 취소됩니다. previous/vote는 'up'|'down'|'none'.",
)
def vote(request: VoteRequest):
    valid = {"up", "down", "none"}
    vote_value = request.vote if request.vote in valid else "none"
    previous = request.previous if request.previous in valid else "none"

    if USE_DUMMY:
        return {
            "message": f"info_id {request.info_id} 투표 완료 (더미)",
            "info_id": request.info_id,
            "vote": vote_value,
            "upvotes": 0,
            "downvotes": 0,
        }

    database = _require_db()

    try:
        result = database.apply_vote(request.info_id, previous, vote_value)
        if result is None:
            raise HTTPException(status_code=404, detail=f"info_id {request.info_id}를 찾을 수 없습니다.")

        return {
            "message": "투표가 반영되었습니다.",
            "info_id": result["info_id"],
            "vote": vote_value,
            "upvotes": result["upvotes"],
            "downvotes": result["downvotes"],
        }
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"투표 처리 중 오류 발생: {exc}") from exc


@app.get(
    "/api/info/{info_id}/comments",
    summary="장단점 댓글 조회",
    description="해당 핵심 정보(info_id)에 달린 댓글 목록을 반환합니다. author_token이 작성자와 일치하면 is_mine=True로 표시합니다.",
)
def get_info_comments(info_id: int, author_token: Optional[str] = None):
    if USE_DUMMY:
        return {"comments": []}

    database = _require_db()
    try:
        return {"comments": database.get_comments(info_id, viewer_token=author_token)}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"댓글 조회 중 오류 발생: {exc}") from exc


@app.post(
    "/api/info/{info_id}/comments",
    summary="장단점 댓글 작성",
    description="해당 핵심 정보(info_id)에 댓글을 추가합니다.",
)
def add_info_comment(info_id: int, request: CommentRequest):
    content = (request.content or "").strip()
    if not content:
        raise HTTPException(status_code=400, detail="댓글 내용이 비어 있습니다.")

    if USE_DUMMY:
        return {"comment": {"comment_id": 0, "info_id": info_id, "content": content, "created_at": "", "is_mine": True}}

    database = _require_db()
    try:
        comment = database.add_comment(info_id, content, author_token=request.author_token)
        if comment is None:
            raise HTTPException(status_code=404, detail=f"info_id {info_id}를 찾을 수 없습니다.")
        return {"comment": comment}
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"댓글 작성 중 오류 발생: {exc}") from exc


@app.put(
    "/api/comments/{comment_id}",
    summary="댓글 수정(작성자 본인)",
    description="author_token이 작성자와 일치할 때만 댓글 내용을 수정합니다.",
)
def update_comment(comment_id: int, request: CommentUpdateRequest):
    content = (request.content or "").strip()
    if not content:
        raise HTTPException(status_code=400, detail="댓글 내용이 비어 있습니다.")

    if USE_DUMMY:
        return {"comment": {"comment_id": comment_id, "content": content, "created_at": "", "is_mine": True}}

    database = _require_db()
    try:
        comment = database.update_comment(comment_id, content, request.author_token)
        if comment is None:
            raise HTTPException(status_code=403, detail="본인 댓글만 수정할 수 있습니다.")
        return {"comment": comment}
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"댓글 수정 중 오류 발생: {exc}") from exc


@app.delete(
    "/api/comments/{comment_id}",
    summary="댓글 삭제(작성자 본인)",
    description="author_token이 작성자와 일치할 때만 댓글을 삭제합니다.",
)
def delete_comment(comment_id: int, author_token: str = Query(...)):
    if USE_DUMMY:
        return {"ok": True}

    database = _require_db()
    try:
        deleted = database.delete_comment(comment_id, author_token)
        if not deleted:
            raise HTTPException(status_code=403, detail="본인 댓글만 삭제할 수 있습니다.")
        return {"ok": True}
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"댓글 삭제 중 오류 발생: {exc}") from exc
