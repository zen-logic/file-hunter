import asyncio

from starlette.applications import Starlette
from starlette.routing import Route, Mount, WebSocketRoute
from starlette.staticfiles import StaticFiles
from pathlib import Path

from file_hunter.db import get_db, close_db
from file_hunter.ws.scan import ws_endpoint
from file_hunter.routes.locations import (
    list_locations,
    add_location,
    remove_location,
    update_location,
    create_new_folder,
    folder_move,
    tree_children,
    tree_expand,
    treemap_data,
)
from file_hunter.routes.files import (
    files_list,
    file_detail,
    file_content,
    file_bytes,
    file_update,
    file_delete,
    file_move,
    file_dup_counts,
    folder_download,
    location_download,
    folder_delete,
    folder_dup_exclude,
)
from file_hunter.routes.search import (
    search,
    list_saved_searches,
    create_saved_search,
    delete_saved_search,
)
from file_hunter.routes.slideshow import slideshow_ids
from file_hunter.routes.scan import start_scan, cancel_scan, get_scan_queue
from file_hunter.routes.consolidate import consolidate, batch_consolidate
from file_hunter.routes.merge import merge, cancel_merge
from file_hunter.routes.upload import upload_files
from file_hunter.routes.stats import stats, location_stats, folder_stats
from file_hunter.routes.browse import browse
from file_hunter.routes.batch import (
    batch_delete_route,
    batch_move_route,
    batch_tag_route,
    batch_download_route,
)
from file_hunter.routes.auth import (
    auth_status,
    auth_setup,
    auth_login,
    auth_logout,
    auth_me,
    list_users,
    create_user,
    update_user,
    delete_user,
)
from file_hunter.routes.settings import (
    get_settings,
    update_settings,
    get_version,
    get_pro_status,
)
from file_hunter.routes.ignore import (
    list_ignore_rules,
    create_ignore_rule,
    delete_ignore_rule,
    check_ignore,
    count_ignore_matches,
)
from file_hunter.routes.update import (
    check_update,
    install_update,
    upload_update,
    restart_server,
)
from file_hunter.middleware import AuthMiddleware
from file_hunter import extensions

static_dir = Path(__file__).resolve().parent.parent / "static"

# Load pro package if installed
try:
    import file_hunter_pro

    file_hunter_pro.register()
except ImportError:
    pass
except Exception:
    import traceback

    traceback.print_exc()


async def on_startup():
    db = await get_db()
    from file_hunter.services.sizes import populate_all_sizes_if_needed

    await populate_all_sizes_if_needed(db)
    from file_hunter.services.scheduler import start_scheduler

    await start_scheduler()
    for hook in extensions.get_startup_hooks():
        await hook()

    from file_hunter.services.scan_queue import restore_queue

    await restore_queue()

    from file_hunter.services.dup_exclude import restore_pending as restore_dup_exclude

    await restore_dup_exclude()

    from file_hunter.services.dup_counts import backfill_dup_counts

    asyncio.get_event_loop().create_task(backfill_dup_counts())


async def on_shutdown():
    from file_hunter.services.scanner import cancel_all_scans
    from file_hunter.services.scan_queue import clear_queue

    clear_queue()
    await cancel_all_scans()
    await close_db()


app = Starlette(
    on_startup=[on_startup],
    on_shutdown=[on_shutdown],
    routes=[
        Route("/api/auth/status", auth_status, methods=["GET"]),
        Route("/api/auth/setup", auth_setup, methods=["POST"]),
        Route("/api/auth/login", auth_login, methods=["POST"]),
        Route("/api/auth/logout", auth_logout, methods=["POST"]),
        Route("/api/auth/me", auth_me, methods=["GET"]),
        Route("/api/auth/users", list_users, methods=["GET"]),
        Route("/api/auth/users", create_user, methods=["POST"]),
        Route("/api/auth/users/{id:int}", update_user, methods=["PATCH"]),
        Route("/api/auth/users/{id:int}", delete_user, methods=["DELETE"]),
        Route("/api/version", get_version, methods=["GET"]),
        Route("/api/settings", get_settings, methods=["GET"]),
        Route("/api/settings", update_settings, methods=["PATCH"]),
        Route("/api/pro/status", get_pro_status, methods=["GET"]),
        Route("/api/update/check", check_update, methods=["POST"]),
        Route("/api/update/install", install_update, methods=["POST"]),
        Route("/api/update/upload", upload_update, methods=["POST"]),
        Route("/api/restart", restart_server, methods=["POST"]),
        Route("/api/locations", list_locations, methods=["GET"]),
        Route("/api/locations", add_location, methods=["POST"]),
        Route("/api/locations/{id:int}/download", location_download, methods=["GET"]),
        Route("/api/locations/{id}", update_location, methods=["PATCH"]),
        Route("/api/locations/{id}", remove_location, methods=["DELETE"]),
        Route("/api/treemap/{id:int}", treemap_data, methods=["GET"]),
        Route("/api/tree/children", tree_children, methods=["GET"]),
        Route("/api/tree/expand", tree_expand, methods=["GET"]),
        Route("/api/files/dup-counts", file_dup_counts, methods=["POST"]),
        Route("/api/files", files_list, methods=["GET"]),
        Route("/api/files/{id:int}/content", file_content, methods=["GET"]),
        Route("/api/files/{id:int}/bytes", file_bytes, methods=["GET"]),
        Route("/api/files/{id:int}", file_detail, methods=["GET"]),
        Route("/api/files/{id:int}", file_update, methods=["PATCH"]),
        Route("/api/files/{id:int}/move", file_move, methods=["POST"]),
        Route("/api/files/{id:int}", file_delete, methods=["DELETE"]),
        Route("/api/folders", create_new_folder, methods=["POST"]),
        Route("/api/folders/{id:int}/download", folder_download, methods=["GET"]),
        Route("/api/folders/{id:int}/move", folder_move, methods=["POST"]),
        Route(
            "/api/folders/{id:int}/dup-exclude", folder_dup_exclude, methods=["POST"]
        ),
        Route("/api/folders/{id:int}", folder_delete, methods=["DELETE"]),
        Route("/api/search", search, methods=["GET"]),
        Route("/api/searches", list_saved_searches, methods=["GET"]),
        Route("/api/searches", create_saved_search, methods=["POST"]),
        Route("/api/searches/{id:int}", delete_saved_search, methods=["DELETE"]),
        Route("/api/slideshow-ids", slideshow_ids, methods=["GET"]),
        Route("/api/scan", start_scan, methods=["POST"]),
        Route("/api/scan/cancel", cancel_scan, methods=["POST"]),
        Route("/api/scan/queue", get_scan_queue, methods=["GET"]),
        Route("/api/consolidate", consolidate, methods=["POST"]),
        Route("/api/batch/consolidate", batch_consolidate, methods=["POST"]),
        Route("/api/merge", merge, methods=["POST"]),
        Route("/api/merge/cancel", cancel_merge, methods=["POST"]),
        Route("/api/upload", upload_files, methods=["POST"]),
        Route("/api/browse", browse, methods=["GET"]),
        Route("/api/batch/delete", batch_delete_route, methods=["POST"]),
        Route("/api/batch/move", batch_move_route, methods=["POST"]),
        Route("/api/batch/tag", batch_tag_route, methods=["POST"]),
        Route("/api/batch/download", batch_download_route, methods=["POST"]),
        Route("/api/ignore", list_ignore_rules, methods=["GET"]),
        Route("/api/ignore", create_ignore_rule, methods=["POST"]),
        Route("/api/ignore/check", check_ignore, methods=["GET"]),
        Route("/api/ignore/count", count_ignore_matches, methods=["GET"]),
        Route("/api/ignore/{id:int}", delete_ignore_rule, methods=["DELETE"]),
        Route("/api/stats", stats, methods=["GET"]),
        Route("/api/locations/{id:int}/stats", location_stats, methods=["GET"]),
        Route("/api/folders/{id:int}/stats", folder_stats, methods=["GET"]),
        WebSocketRoute("/ws", ws_endpoint),
        *extensions.get_routes(),
        *[
            Mount(path, app=StaticFiles(directory=d))
            for path, d in extensions.get_static_mounts().items()
        ],
        Mount("/", app=StaticFiles(directory=static_dir, html=True)),
    ],
)

app = AuthMiddleware(app)
