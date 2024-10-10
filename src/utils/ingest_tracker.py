# ingest_tracker.py

from .logger import setup_logger
from sqlalchemy import create_engine, Column, String, Integer, DateTime
from sqlalchemy.orm import sessionmaker, declarative_base
from datetime import datetime

Base = declarative_base()


class IngestionTracking(Base):
    __tablename__ = 'ingestion_tracking'

    id = Column(Integer, primary_key=True)
    group_name = Column(String, nullable=False)
    user_name = Column(String, nullable=False)
    data_package = Column(String, nullable=False)
    stage = Column(String, nullable=False)
    uuid = Column(String, nullable=False)
    timestamp = Column(DateTime, default=datetime.utcnow)


class IngestTracker:
    def __init__(self, config):
        self.database_url = config['ingest_tracking_db']
        self.logger = setup_logger(__name__, config['log_file_path'])
        self.engine = create_engine(self.database_url)
        self.Session = sessionmaker(bind=self.engine)
        # Create tables if they don't exist
        Base.metadata.create_all(self.engine)
        
    def __exit__(self, exc_type, exc_value, traceback):
        """Clean up resources when the class instance is destroyed or goes out of scope."""
        self.dispose()

    def dispose(self):
        """Dispose of the database session and logger resources."""
        # Close the logger handlers
        for handler in self.logger.handlers:
            handler.close()
            self.logger.removeHandler(handler)

        # Dispose of the database engine
        self.engine.dispose()

    def log_ingestion_step(self, group, user, dataset, stage, uuid):
        """Log an ingestion step to the database."""
        with self.Session() as session:
            try:
                new_entry = IngestionTracking(
                    group_name=group,
                    user_name=user,
                    data_package=dataset,
                    stage=stage,
                    uuid=str(uuid)
                )
                session.add(new_entry)
                session.commit()
                self.logger.info(f"Logged ingestion step: {group}, {user}, {dataset}, {stage}, {uuid}")
                return new_entry.id
            except Exception as e:
                session.rollback()
                self.logger.error(f"Error logging ingestion step: {e}")
                return None


# Global instance of IngestTracker
ingest_tracker = None


def initialize_ingest_tracker(config):
    """Initialize the global IngestTracker instance."""
    global ingest_tracker
    ingest_tracker = IngestTracker(config)


def log_ingestion_step(group, user, dataset, stage, uuid):
    """Global function to log ingestion steps using the IngestTracker instance."""
    if ingest_tracker is not None:
        return ingest_tracker.log_ingestion_step(group, user, dataset, stage, uuid)
    else:
        print("Error: IngestTracker not initialized. Call initialize_ingest_tracker first.")
