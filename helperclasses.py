from dataclasses import dataclass

@dataclass
class TickerData:
    ask = float
    bid = float
    mid = float
    delta = float
    gamma = float
    theta = float
    implied_vol = float

    def print(self):
        return "Ask: {}\n Bid: {}\n Mid: {}\ Delta: {}\n Gamma: {}\n Theta: {}\n Implied Vol: {}"\
            .format(self.ask, self.bid, self.mid, self.delta, self.gamma, self.theta, self.implied_vol)