from lightstreamer.client import LightstreamerClient, Subscription, SubscriptionListener,ConsoleLoggerProvider, ConsoleLogLevel
from utils import *
from datetime import datetime, timedelta

spark = create_spark_session()


LS_SERVER = "https://push.lightstreamer.com"   # Public Lightstreamer broker
ADAPTER_SET = "ISSLIVE"


ITEMS = [
    "NODE3000008",   # Number of CMGs online (example)
    "NODE3000005",   # Urine tank level [%] (example)
    "AIRLOCK000007", # Airlock 7 pressure (example)
]
FIELDS = [
    "TimeStamp",
    "Value",
]


REQUEST_SNAPSHOT = "yes"
result = []

# --- Listener to print updates ---
class ISSSubListener(SubscriptionListener):
    def onSubscription(self):
        print("✅ Subscribed to ISS telemetry.")

    def onUnsubscription(self):
        print("ℹ️ Unsubscribed.")

    def onSubscriptionError(self, code, message):
        print(f"❌ Subscription error {code}: {message}")

    def onItemUpdate(self, update):
        item = update.getItemName()
        # Print everything we got for quick introspection
        field_values = {}
        for f in FIELDS:
            # getValue returns None if the field name doesn't exist for this item
            field_values[f] = update.getValue(f)
        result.append((item, field_values))
        print(f"📡 {item}: {field_values}")

def main():
    # Create client and connect
    client = LightstreamerClient(LS_SERVER, ADAPTER_SET)
    client.connect()

    # Build a MERGE subscription for selected items and fields
    sub = Subscription("MERGE", ITEMS, FIELDS)
    sub.setRequestedSnapshot(REQUEST_SNAPSHOT)
    sub.addListener(ISSSubListener())

    # Send subscription
    client.subscribe(sub)

    try:
        print("🔌 Connected. Press Ctrl+C to stop ...")
        while True:
            if len(result) >=50:
                df = spark.createDataFrame(result, schema=["item", "fields"])
                df.write.format("delta").mode("append").save("/home/nagendravippala/ISS_Telemetry/catalog/bronze/iss_telemetry")
                result.clear()  # Clear the list to avoid reprocessing the same data
            pass
    except KeyboardInterrupt:
        print("\nStopping…")
    finally:
        client.unsubscribe(sub)
        client.disconnect()

if __name__ == "__main__":
    main()


