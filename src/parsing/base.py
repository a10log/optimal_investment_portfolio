from dataclasses import dataclass

@dataclass
class PathsList:
    
    instruments_path: str = "data/instruments.csv"
    raw_dynamics_path = "./data/raw_dynamics_data/"
    raw_dividends_path: str = "./data/raw_dividends_data.csv"