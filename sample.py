# Get database objects
from db import SessionLocal
from models import Metric, SeenEvent, TaskEntry
session = SessionLocal()


# Example queries
[ r.__dict__ for r in session.query(SeenEvent).all() ]
[ r.__dict__ for r in session.query(Metric).filter(Metric.endpoint=="daily_sleep").all() ]
[ r.__dict__ for r in session.query(TaskEntry).limit(10) ]


# Delete seen events
# session.query(SeenEvent).delete(); session.commit()
# session.query(Metric).delete(); session.commit()