// MongoDB schema, the counterpart to the SQL init.sql files. Collection and field names match them
// exactly. Run once by the image's entrypoint, against MONGO_INITDB_DATABASE, on a fresh volume.

// A time-series collection: MongoDB's own layout for this workload, bucketing readings per device
// the way a hypertable chunks them. timeField must be a BSON Date, which holds milliseconds, so the
// stored Timestamp loses the payload's microseconds.
db.createCollection("Data", {
  timeseries: { timeField: "Timestamp", metaField: "DeviceName", granularity: "seconds" }
});
// No validator: MongoDB refuses one on a time-series collection, so nothing here plays the part of
// NOT NULL or an INT column. A backend that sent Value as a double would be stored silently, which is
// why the MongoDB smoke-test appendix checks the stored type.

// The counterpart of the SQL CHECK on Status. reportedat is sent by the client, since MongoDB has
// no insert-time default.
db.createCollection("sensor_status", {
  validator: {
    $jsonSchema: {
      required: ["DeviceName", "Status", "reportedat"],
      properties: {
        DeviceName: { bsonType: "string" },
        Status: { enum: ["ALIVE", "MISSING"] },
        reportedat: { bsonType: "date" }
      }
    }
  }
});
db.sensor_status.createIndex({ reportedat: 1 });
