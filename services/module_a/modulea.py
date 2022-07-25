import pandas
import json
import redis
import numpy as np

with open('config_file.json') as f:
        CONFIG = json.load(f)
class Main:
    def __init__(self, dates: str):
        self.dates = [dates]
        self.total_customers_dict = Utils.load_customers().to_dict('records')


    def __get_redis_connection(self):
        r = redis.Redis(host='redis-14047.c17.us-east-1-4.ec2.cloud.redislabs.com', port=14047, db=0, password='SwgBhJqaRP6Qph0KCjqyGfBH3kuCiiO8')
        return r


    def run(self):
        payment_handler = PaymentHandler()
        customers = [Customer(**customer) for customer in self.total_customers_dict]
        for customer in customers:
            payments = payment_handler.obtain_from_cid(cid=customer.id_)
            customer.set_payment(payments)
        output = [customer.serialize() for customer in customers]
        output_b = json.dumps(output, cls=PandasEncoder)
        conn = self.__get_redis_connection()
        conn.set('payments', output_b)
        dates_str = self.dates[0]
        conn.set('dates', dates_str)



class PandasEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, np.integer):
            return int(obj)
        return super(PandasEncoder, self).default(obj)


class PaymentHandler:
    def __init__(self):
        self.total_payments_df = Utils.load_payments()
        self.summarized_payments_df = self.__sum_payments()


    def obtain_from_cid(self, cid: int) -> dict:
        dates = [date for date in self.total_payments_df['date'].unique()]
        payments_dict = dict()
        for n, data in enumerate(dates):
            payments_dict.update({data:self.summarized_payments_df[cid][n]})
        return payments_dict


    def __sum_payments(self):
        try:
            payments_df=self.total_payments_df.groupby(['id_customer','date']).sum('payment')['payment']
        except Exception as ex:
            print("DEBUG ", ex)
            return None
        return payments_df


class Customer:
    def __init__(self, **kwargs):
        self.id_ = kwargs.get('id_customer')
        self.age = kwargs.get('age')
        self.job = kwargs.get('job')
        self.marital = kwargs.get('marital')
        self.education = kwargs.get('education')
        self.payments = list()


    def serialize(self):
        return {
            'id_customer':self.id_,
            'age':self.age,
            'job':self.job,
            'marital':self.marital,
            'education':self.education,
            'payments':self.payments
        }


    def set_payment(self, payment):
        self.payments = payment


class Utils:
    @staticmethod
    def load_customers() -> pandas.DataFrame:
        try:
            return pandas.read_csv(CONFIG['dataset_customers'],sep=';')
        except Exception as ex:
            print("Error", ex)
            return None


    @staticmethod
    def load_payments() -> pandas.DataFrame:
        try:
            return pandas.read_csv(CONFIG['dataset_payments'], sep=';')
        except Exception as ex:
            print("Error", ex)
            return None


if __name__ == "__main__":
    date = CONFIG['FECHA_PERIODO']
    main = Main(date)
    main.run()


