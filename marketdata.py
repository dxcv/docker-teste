from event import Event
from genericEvent import GenericEvent
from datetime import datetime
import pandas as pd
import os

# (covmatrix * signal vector) * signal transpose


class MarketData():

    TICK, HIST, INTR = ['TICK', 'HIST', 'INTR']

    def __init__(self):

        self.events = {}
        self.cov_events = {}  # for multiple dates, change this to a dict of lists

        self.loaded_files = []

    def loadBBGTick(self, file, instrument):
        print('file: ', file)

        if(file in self.loaded_files):
            print("WARNING: Tried to read the same file more than once")
            return

        self.loaded_files.append(file)

        with open(file, 'r') as file:
            data = file.read()

        events = data.split('\n')
        events = events[1:]
        for event in events:
            cols = event.split(';')
            if len(cols) == 4:
                date = datetime.strptime(cols[0], '%d/%m/%Y %H:%M:%S')
                #print('date: ', date)
                price = float(cols[2].replace(',', '.'))
                #print('price: ', price)
                quantity = int(cols[3])
                #print('quantity: ', quantity)
                type = cols[1]
                #print('type: ', type)

                if date.toordinal() not in self.events:
                    self.events[date.toordinal()] = []
                # print(date.toordinal())
                self.events[date.toordinal()].append(
                    Event(instrument, date, type, price, quantity))

    def Str_to_Unix(self, String):
        Date = datetime.strptime(String, "%Y-%m-%d %H:%M:%S.%f")
        return (Date - datetime(1970, 1, 1)).total_seconds()

    def loadFTP(self):
        print("Loading files")
        data_files = []
        for r_dummy, d_dummy, f in os.walk("./data"):
            for file in f:
                data_files.append(file)
        full_data = {}

        for data_file in data_files:

            instrument = data_file.split("_")[-1].split(".")[0]
            is_neg = data_file.split("_")[1] == "NEG"
            is_cpa = data_file.split("_")[2] == "CPA"
            is_vda = data_file.split("_")[2] == "VDA"
            is_cov = data_file.split("_")[1] == "COV"

            offer_type = None
            if(is_cpa or is_vda):
                offer_type = "VDA" if (data_file.split("_")[
                                       2]) == "VDA" else "CPA"

            print(f'loading {data_file} - instrument:', instrument)
            data = pd.read_parquet("./data/" + data_file)

            if(is_neg):
                # Reitera colunas inuteis
                data = data.drop(["Nr.Negocio", "Anulacao", "Data CPA", "Data CPA", "Num CPA", "GenerationID CPA", "Condicao CPA",
                                  "Data VDA", "Num VDA", "GenerationID VDA", "Condicao VDA",
                                  "Direto", "Comprador", "Vendedor"], axis=1)
                data = data.rename(columns={"Quantidade": "Volume"})
            # Restam as colunas0 ["Data Sessao", "Ativo", "Preço", "Volume", "Hora"]
                data["Tipo"] = "TRADE"
                day = data["Data Sessao"][0]

            if(is_cpa or is_vda):
                # Reitra colunas inuteis
                data = data.drop(["Sentido", f"Num {offer_type}", f"GenerationID {offer_type}", "Evento", "Estado", "Condicao", "Corretora", "Ind de Prioridade",
                                  "Qtd.Negociada", "Data Oferta", "Data de Entrada"], axis=1)

                # Renomeia
                data = data.rename(
                    columns={"Hora Prioridade": "Hora", "Qtd.Total": "Volume"})
                # Restam as colunas0 ["Data Sessao", "Ativo", "Preço", "Quantidade", "Hora"]
                data["Tipo"] = "ASK" if offer_type == "VDA" else "BID"

                day = data["Data Sessao"][0]

            if(is_cov):
                # iterate the dataframe and make an event for each combination?
                data["new_index"] = data.columns
                data = data.set_index("new_index")
                print(data.head(20))
                self.cov_events = data

            # No fim temos as colunas ["Data Sessao", "Ativo", "Preço", "Quantidade", "Hora", "Tipo"]
            # concaterna os dataframes de mesma data(dia) no full_data
            if not day in full_data.keys():
                full_data[day] = data
            else:
                full_data[day] = pd.concat([full_data[day], data], sort=False)

        '''
    print("Veremos as datas")
    for k in full_data.keys():
      print(full_data[k].head(10))
    '''

        # Ordenar
        print("Ordenando")
        start = datetime.now()
        for k in full_data.keys():
            full_data[k] = full_data[k].sort_values(
                "Hora", axis=0, ascending=True, kind="quicksort")
        #data = data.sort_values("Hora", axis=0, ascending = True, kind = "quicksort")
        print("Ordenado!!!")
        print("tempo para ordenar= ", datetime.now() - start)

        print("Veremos tudo ordenado")
        for k in full_data.keys():
            print(full_data[k].head(25))

        # print(data.head(5))

        print("Adicionando eventos")
        for k in full_data.keys():
            print("Adicionando eventos do dia " + k)
            # colocar .iloc[:n] na frente do full_data[k] para reduzir a amostra
            for index_dummy, row in full_data[k].iloc[9000:10000].iterrows():
                #print (row)
                date = datetime.strptime(
                    row["Data Sessao"] + " " + row["Hora"], '%Y-%m-%d %H:%M:%S.%f')
                # print(date)
                price = row["Preco"]
                # print(price)
                quantity = row["Volume"]
                # print(quantity)
                event_type = row["Tipo"]
                # print(type)

                if date.toordinal() not in self.events:
                    self.events[date.toordinal()] = []

                # print(date.toordinal())
                #self.events[date.toordinal()].append(Event(instrument, date, type, price, quantity))
                self.events[date.toordinal()].append(
                    Event(instrument, date, event_type, price, quantity))
        print("Adicionados")

    def loadYAHOOHist(self, file, instrument, type=Event.CANDLE):

        with open(file, 'r') as file:
            data = file.read()

        events = data.split('\n')
        events = events[1:]
        for event in events:
            cols = event.split(',')
            if len(cols) == 7 and cols[1] != 'null':

                date = datetime.strptime(cols[0], '%Y-%m-%d')
                price = (float(cols[1]), float(cols[2]),
                         float(cols[3]), float(cols[5]))
                quantity = int(cols[6])

                if date.toordinal() not in self.events:
                    self.events[date.toordinal()] = []

                self.events[date.toordinal()].append(
                    Event(instrument, date, type, price, quantity))

    def loadBBGIntr(self, file, instrument, type=Event.CANDLE):

        with open(file, 'r') as file:
            data = file.read()

        events = data.split('\n')
        events = events[1:]
        for event in events:
            cols = event.split(';')
            if len(cols) == 5:

                date = datetime.strptime(cols[0], '%d/%m/%Y %H:%M:%S')
                price = (float(cols[1].replace(',', '.')),
                         float(cols[3].replace(',', '.')),
                         float(cols[4].replace(',', '.')),
                         float(cols[2].replace(',', '.')))
                quantity = 0

                if date.timestamp() not in self.events:
                    self.events[date.timestamp()] = []

                self.events[date.timestamp()].append(
                    Event(instrument, date, type, price, quantity))

    # def loadCOV(self):
    #     print("Loading COV file")
    #     data_files = []
    #     for r_dummy, d_dummy, f in os.walk("./COV"):
    #         for file in f:
    #             data_files.append(file)
    #     print(data_files)

    #     for data_file in data_files:
    #         cov_data = pd.read_parquet("./COV/" + data_file)
    #         # iterate the dataframe and make an event for each combination?
    #         cov_data["new_index"] = cov_data.columns
    #         cov_data = cov_data.set_index("new_index")
    #         print(cov_data.head(20))
    #         self.cov_events = cov_data
    #         # for (instrument, covs) in cov_data.iteritems():
    #         #print("instrument ", instrument)
    #         #print("covs ", covs)
    #         #self.cov_events[instrument] = covs

    def run(self, ts):
        print("Running!!!")

        # self.loadCOV()

        dates = list(self.events.keys())
        dates.sort()
        print('dates: ', dates)
        for date in dates:
            ts.injectCOV(GenericEvent(self.cov_events))
            for event in self.events[date]:
                #print("data " + event.timestamp.strftime("%m/%d/%Y, %H:%M:%S.%f"))
                ts.inject(event)
