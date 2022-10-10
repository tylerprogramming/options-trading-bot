"""
    This is the configuration for servers and variables that are unique to each bot.
"""

database_config = {
    'user': 'root',
    'password': 'adminpassword',
    'host': '127.0.0.1',
    'database': 'trade'
}

redis_port = 6379
localhost_port = 5002
interactive_brokers_port = 7496

BALANCE = 2500
RISK = .02
NUMBER_OF_CONTRACTS = 2
NUMBER_OF_STRIKE_PRICES = 10
PUT_UPPER_DELTA_BOUNDARY = -0.47
PUT_LOWER_DELTA_BOUNDARY = -0.37
CALL_UPPER_DELTA_BOUNDARY = 0.47
CALL_LOWER_DELTA_BOUNDARY = 0.37

