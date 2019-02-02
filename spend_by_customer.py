"""
Run with:

python spend_by_customer.py assets/customer-orders.csv
"""


from mrjob.job import MRJob
from mrjob.step import MRStep

class MRSpendByCustomer(MRJob):
    def steps(self):
        return [
            MRStep(
                mapper=self.mapper_spend_by_customer,
                reducer=self.reducer_spend_by_customer,
            ),
            MRStep(
                mapper=self.mapper_group_by_spend,
                reducer=self.reducer_order_by_spend,
            )
        ]

    def mapper_spend_by_customer(self, key, line):
        (customer_id, item_id, order_amount) = line.split(',')
        yield customer_id, float(order_amount)

    def reducer_spend_by_customer(self, customer_id, order_amount_list):
        yield customer_id, sum(order_amount_list)

    def mapper_group_by_spend(self, customer_id, total):
        yield '%04.02f' % total, customer_id

    def reducer_order_by_spend(self, total, customer_ids):
        for customer_id in customer_ids:
            yield customer_id, total

if __name__ == '__main__':
    MRSpendByCustomer.run()
