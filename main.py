import logging

logging.basicConfig(
    filename='app.log',  # Name of the log file
    level=logging.DEBUG,  # Logging level for capturing messages
    format='%(asctime)s %(levelname)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
