"""
This module represents the Marketplace.

Computer Systems Architecture Course
Assignment 1
March 2021
"""
from threading import Lock


class Marketplace:
    """
    Class that represents the Marketplace. It's the central part of the implementation.
    The producers and consumers use its methods concurrently.
    """
    def __init__(self, queue_size_per_producer):
        """
        Constructor

        :type queue_size_per_producer: Int
        :param queue_size_per_producer: the maximum size of a queue associated with each producer
        """
        self.queue_size_per_producer = queue_size_per_producer
        # number of producers
        self.number_of_producers = 0
        # number of carts
        self.number_of_carts = 0
        # id_prod && list products
        self.producers = {}
        # id_cart && list products
        self.carts = {}
        # available product
        self.product_available = []

        self.lock = Lock()

    def register_producer(self):
        """
        Returns an id for the producer that calls this.
        """
        with self.lock:
            self.number_of_producers += 1
            self.number_of_carts += 1
            self.producers[self.number_of_producers] = []

        return self.number_of_producers

    def publish(self, producer_id, product):
        """
        Adds the product provided by the producer to the marketplace

        :type producer_id: String
        :param producer_id: producer id

        :type product: Product
        :param product: the Product that will be published in the Marketplace

        :returns True or False. If the caller receives False, it should wait and then try again.
        """
        if len(self.producers[producer_id]) == self.queue_size_per_producer:
            return False

        self.producers[producer_id].append(product)

        self.product_available.append(product[0])

        return True

    def new_cart(self):
        """
        Creates a new cart for the consumer

        :returns an int representing the cart_id
        """
        with self.lock:
            self.number_of_carts += 1
            self.carts[self.number_of_carts] = []

        return self.number_of_carts

    def add_to_cart(self, cart_id, product):
        """
        Adds a product to the given cart. The method returns

        :type cart_id: Int
        :param cart_id: id cart

        :type product: Product
        :param product: the product to add to cart

        :returns True or False. If the caller receives False, it should wait and then try again
        """

        with self.lock:
            for elem in self.product_available:
                if product == elem:
                    self.carts[cart_id].append(product)
                    return True
        return False

    def remove_from_cart(self, cart_id, product):
        """
        Removes a product from cart.

        :type cart_id: Int
        :param cart_id: id cart

        :type product: Product
        :param product: the product to remove from cart
        """
        if product in self.carts[cart_id]:
            self.carts[cart_id].remove(product)
            self.product_available.append(product)

            return True

        return False

    def place_order(self, cart_id):
        """
        Return a list with all the products in the cart.

        :type cart_id: Int
        :param cart_id: id cart
        """
        list_products = self.carts[cart_id]
        self.carts.pop(cart_id)
        for product in list_products:
            for list_products_prod in self.producers.values():
                if product in list_products_prod:
                    list_products_prod.remove(product)

        return list_products
