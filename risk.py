from portifolio import Portifolio
from order import Order
import numpy as np
from scipy.optimize import minimize

class Risk(): #should i call it Trader? or something else?

  def __init__(self,_ts):
    self.portifolio = Portifolio()
    self.ts = _ts
    self.var = -1
    
    self.cov_data = {}

    self.last_price = None
    

  
  def set_var(self, value): #values are in cents
    var = int(value) 
    self.var = var
  
  def set_cov(self, cov_data):
    #print("aaa")
    #print(cov_data.head(50))
    self.cov_data = cov_data
    columns = cov_data.columns
    self.signal_vector = {key:value for key, value in zip(columns, [0]*len(columns))}

  def markowitz_opt(self, weights, cov_data):
        
    volat1 = np.dot(weights, cov_data)
    volat2 = np.dot(volat1, np.transpose(weights))
    volat = np.sqrt(abs(volat2))

    return volat

  def send(self, signals):
    #print("im getting the signal at least")
    #here is where we make the magic

    #for now i just apply the signal value as a order quantity and that is it (as it was on exampleMarcelo.py)
    #print(self.ts.currentEvent.price)
    orders = []

    if self.ts.currentEvent.type == "TRADE":
      self.last_price = self.ts.currentEvent.price

    if (False):
      
      for signal in signals:
          amount = self.var / self.last_price
          amount -= amount % 100
          #print(amount)
          orders.append(Order(signal.instrument, signal.value * amount, 0))
      
      self.ts.submit(None,orders)

    elif len(signals) != 0:
      #MARKOVITSCHSKLSI doedu
      for signal in signals:
            self.signal_vector[signal.instrument] = signal.value
      
      weights = list(self.signal_vector.values())
      
      n = len(weights)
      start_weights = [1/n] * n

      cons = ({'type':"eq", 'fun':lambda x: 1-sum(x)})

      bnds = []
      for weight in weights:
        if weight == 1:
          bnds.append((0, weight))
        elif weight == -1:
          bnds.append((weight, 0))
        else:
          bnds.append((0, 0))
      bnds = tuple(bnds)  

      weights = minimize(self.markowitz_opt, start_weights, method="SLSQP", args=(self.cov_data), bounds=bnds, constraints=cons).x
      weights = weights*self.var

      for weight in weights:
        
        if weight != 0:
          amount = weight / self.last_price
          amount -= amount % 100
          orders.append(Order(signal.instrument, amount, 0))
      
      #print("Mandei ele treidar")
      self.ts.submit(None,orders)

      

