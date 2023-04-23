"""
This module represents the Consumer.

Computer Systems Architecture Course
Assignment 1
March 2021
"""
import time
from threading import Thread, Lock


class Consumer(Thread):
    """
    Class that represents a consumer.
    """
    lock = Lock()

    def __init__(self, carts, marketplace, retry_wait_time, **kwargs):
        """
        Constructor.

        :type carts: List
        :param carts: a list of add and remove operations

        :type marketplace: Marketplace
        :param marketplace: a reference to the marketplace

        :type retry_wait_time: Time
        :param retry_wait_time: the number of seconds that a producer must wait
        until the Marketplace becomes available

        :type kwargs:
        :param kwargs: other arguments that are passed to the Thread's __init__()
        """
        Thread.__init__(self, **kwargs)
        self.carts = carts
        self.marketplace = marketplace
        self.retry_wait_time = retry_wait_time
        self.name = kwargs['name']

    def run(self):
        new_cart = self.marketplace.new_cart()
        for cart in self.carts:
            for cart_action in cart:
                # Accesarea datelor
                type_instr = cart_action["type"]
                product = cart_action["product"]
                quantity = cart_action["quantity"]

                if type_instr == "add":
                    while quantity != 0:
                        if self.marketplace.add_to_cart(new_cart, product):
                            quantity -= 1
                        else:
                            # in cazul care produsul nu e disponibil se asteapta
                            time.sleep(self.retry_wait_time)

                if type_instr == "remove":
                    while quantity != 0:
                        self.marketplace.remove_from_cart(new_cart, product)
                        quantity -= 1

        order = self.marketplace.place_order(new_cart)

        with self.lock:
            for product in order:
                print(f"{self.name} bought {product}")
