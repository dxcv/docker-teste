from book import Book
from order import Order
from event import sign
from copy import deepcopy
from strategy import Strategy
from risk import Risk

class TradingSystem():

  def __init__(self):
    self.books = {}    
    self.position = {}
    self.orders = {}
    self.listeners = {}
    self.strategies = {}
    
    #risk
    self.risks = {  } #adding a risk 
    self.currentRisk = 0 #in the future, you can let the user set the risks (than change currentRisk to none and let the user manage this)
    self.currentStrategyId = None
    self.currentEvent = None

  def createRisk(self, var):
    self.currentRisk += 1
    risk = Risk(self)
    risk.set_var(var)
    self.risks[self.currentRisk] = risk


  def createBook(self, instrument):
    if instrument not in self.books:
      self.books[instrument] = Book(instrument, self.fill)

    if instrument not in self.position:
      self.position[instrument] = {}

    if instrument not in self.listeners:
      self.listeners[instrument] = []

  def inject(self, event):
    self.currentEvent = event
    instrument = event.instrument
    #print('instrument: ', instrument)
    if instrument in self.books:
      self.books[instrument].inject(deepcopy(event))


      for id in self.listeners[instrument]:
        if id in self.strategies:
          #save current id for later
          self.currentStrategyId = id

          #TODO - set the Risk id to the correct one (MAYBE I DONT NEED THIS)
          #self.currentRisk = riskId (or something of the sort)

          #dont need to return anymore as a func in Risk will call submit, but send a risk reference with it
          self.strategies[id].event(event, self.risks[self.currentRisk])
          #deprecated
          #self.submit(id, self.strategies[id].event(event))

  def injectCOV(self, gen_event):
    risk = self.risks[self.currentRisk]
    risk.set_cov(gen_event.data)


  #risk
  #def waitEvent(id): #might not need this at all
  #      self.submit(id, orders)

  def subscribe(self, instrument, strategy):
    if strategy.id not in self.strategies:
      self.strategies[strategy.id] = strategy
      strategy.cancel = self.cancel
      strategy.submit = self.submit

    if instrument in self.books:
      if strategy.id not in self.position[instrument]:
        self.position[instrument][strategy.id] = 0

      if strategy.id not in self.listeners[instrument]:
        self.listeners[instrument].append(strategy.id)

  def submit(self, id, orders):

    #risk
    if id == None:
      id = self.currentStrategyId
    #end risk

    
    #print("recebi a ordem")

    for order in orders:
      
      order.owner = id
      instrument = order.instrument

      if instrument in self.position:
        if id in self.position[instrument]:
          position = self.position[instrument][id]

      if sign(position) * sign(position + order.quantity) == -1:
        order.status = Order.REJECTED
        if id in self.strategies:
          strategy = self.strategies[id]
          strategy.fill(instrument, 0, 0, order.status)
      else:
        if order.id not in self.orders:
          self.orders[order.id] = order

        if instrument in self.books:
          #print("minha vide eh feliz")
          self.books[instrument].submit(order)

  def cancel(self, owner, id):
    if id in self.orders:
      if self.orders[id].owner == owner:
        instrument = self.orders[id].instrument
        if instrument in self.books:
          self.books[instrument].cancel(id)

  def fill(self, id, price, quantity, status):
    
    if id in self.orders:

      order = self.orders[id]
      instrument = order.instrument
      owner = order.owner

      if instrument in self.position:
        if owner in self.position[instrument]:
          self.position[instrument][owner] += quantity

      if owner in self.strategies:
        strategy = self.strategies[owner]
        strategy.fill(instrument, price, quantity, status)