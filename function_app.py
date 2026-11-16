import azure.functions as func
import logging
from datetime import datetime, timezone

app = func.FunctionApp()

@app.timer_trigger(
    schedule="0 0 * * * *",
    arg_name="timer",
    run_on_startup=False,
    use_monitor=True
)
def collect_poe2_prices(timer: func.TimerRequest) -> None:
    logging.info("PoE 2 currency collector started")
    logging.info(
        "Execution time: %s",
        datetime.now(timezone.utc).isoformat()
    )
    logging.info("PoE 2 currency collector completed")
