# Databricks notebook source
# MAGIC %restart_python

# COMMAND ----------

# MAGIC %pip install lightstreamer-client-lib
# MAGIC import sys
# MAGIC if 'dbutils' in globals():
# MAGIC     dbutils.library.restartPython()
# MAGIC

# COMMAND ----------

# MAGIC %run /Workspace/Users/dattada.vijay@gmail.com/.bundle/Cavallo_hackathon/dev/files/src/config

# COMMAND ----------

from datetime import datetime
import os, json, threading, queue, time, sys

# COMMAND ----------

from lightstreamer.client import LightstreamerClient, Subscription, SubscriptionListener

event_q = queue.Queue()
stop_flag = threading.Event()


def _write_jsonlines(records, base_dir):
    """Write records to a new NDJSON file via atomic rename."""
    if not records:
        return None
    ts = datetime.utcnow().strftime("%Y%m%dT%H%M%S%f")
    tmp_path = os.path.join(base_dir, f".ls_{ts}.json.tmp")
    final_path = os.path.join(base_dir, f"ls_{ts}.json")
    with open(tmp_path, "w") as f:
        for rec in records:
            f.write(json.dumps(rec) + "\n")
    os.rename(tmp_path, final_path)  # atomic move so Spark sees whole files
    return final_path

def flush_loop(flush_interval_sec=3, max_batch=500):
    """
    Drain the queue and write NDJSON files on either:
      - reaching max_batch, OR
      - flush_interval_sec elapsing since last flush.
    Flush any remaining records on shutdown.
    """
    buffer = []
    last_flush = time.time()
    while not stop_flag.is_set():
        # Compute remaining wait to honor flush_interval_sec
        timeout = max(0.1, flush_interval_sec - (time.time() - last_flush))
        try:
            item = event_q.get(timeout=timeout)
            buffer.append(item)
        except queue.Empty:
            pass  # no new items during this wait window

        time_expired = (time.time() - last_flush) >= flush_interval_sec
        size_reached = len(buffer) >= max_batch

        if buffer and (time_expired or size_reached):
            path = _write_jsonlines(buffer, RAW_DIR)
            print(f"[FLUSH] wrote {len(buffer)} records -> {path}")
            buffer.clear()
            last_flush = time.time()

    # Final flush on shutdown
    if buffer:
        path = _write_jsonlines(buffer, RAW_DIR)
        print(f"[FLUSH-FINAL] wrote {len(buffer)} records -> {path}")

flusher = threading.Thread(target=flush_loop, kwargs={"flush_interval_sec":3, "max_batch":500}, daemon=True)
flusher.start()

# ---- 3b) Lightstreamer subscription listener
class ISSLive(SubscriptionListener):
    
    def onSubscriptionError(self, code, message):
        print(f"Subscription error: {code} - {message}")

    def onSubscription(self):
        print("subscribed")

    def onItemUpdate(self, update):
        # transform each update -> a normalized dict
        # print(f"Incoming data: {update}")
        rec = {
            "item_name": update.getItemName(),
            "timestamp": update.getValue("TimeStamp"),
            "value": update.getValue("Value")
        }
        event_q.put(rec)
        # return rec

# ---- 3c) connect & subscribe (public demo server)
client = LightstreamerClient("https://push.lightstreamer.com", "ISSLIVE")
sub = Subscription(
    "MERGE",
    field_names,
    ["TimeStamp","Value"]
)

sub.setRequestedSnapshot("yes")
sub.addListener(ISSLive())
client.connect()
client.subscribe(sub)
# time.sleep(30)



# COMMAND ----------

client.unsubscribe(sub)
client.disconnect()

stop_flag.set()
flusher.join(timeout=5)