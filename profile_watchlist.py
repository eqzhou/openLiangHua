import cProfile
import pstats
from src.app.facades.watchlist_facade import get_watchlist_summary_payload

def main():
    get_watchlist_summary_payload(
        keyword="",
        scope="all",
        sort_by="inference_rank",
        page=1,
        include_realtime=False
    )

if __name__ == "__main__":
    profiler = cProfile.Profile()
    profiler.enable()
    main()
    profiler.disable()
    stats = pstats.Stats(profiler).sort_stats('cumtime')
    stats.print_stats(30)
