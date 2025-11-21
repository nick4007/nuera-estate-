from neuraestate.db.base import Base, engine
from neuraestate.logging_setup import setup_logging
import logging
from neuraestate.db import models

def main() -> None:
    # initialize logging (console + logs/neuraestate.log)
    setup_logging()
    logger = logging.getLogger("neuraestate")

    Base.metadata.create_all(engine)
    logger.info("✅ Tables created successfully.")

if __name__ == "__main__":
    main()
