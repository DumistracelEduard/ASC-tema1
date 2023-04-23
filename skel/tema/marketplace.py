"""
This module represents the Marketplace.

Computer Systems Architecture Course
Assignment 1
March 2021
"""
import logging
import time
import unittest
from logging.handlers import RotatingFileHandler
from threading import Lock

logging.basicConfig(
    handlers=[RotatingFileHandler('./marketplace.log', maxBytes=100000, backupCount=5)],
    level=logging.INFO,
    format="%(asctime)s %(levelname)s: %(message)s"
)
logging.Formatter.converter = time.gmtime


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

        self.products = {}

        self.lock = Lock()

    def register_producer(self):
        """
        Returns an id for the producer that calls this.
        """
        # id-urile incep de la 1
        with self.lock:
            self.number_of_producers += 1
            self.number_of_carts += 1
            self.producers[self.number_of_producers] = []
            self.products[self.number_of_producers] = []
            logging.info("register prod id = %s", self.number_of_producers)

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
        # in cazul in care are numarul maxim de produse
        if len(self.producers[producer_id]) == self.queue_size_per_producer:
            logging.info("%s product: %s full", producer_id, product)
            return False

        with self.lock:
            logging.info("%s product: %s", producer_id, product[0])
            self.producers[producer_id].append(product[0])
            self.products[producer_id].append(product[0])
        return True

    def new_cart(self):
        """
        Creates a new cart for the consumer

        :returns an int representing the cart_id
        """
        # id-urile incep de la 1
        with self.lock:
            self.number_of_carts += 1
            logging.info("new_cart_id: %s", self.number_of_carts)
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
            for product_list in self.producers.values():
                if product in product_list:
                    product_list.remove(product)
                    self.carts[cart_id].append(product)
                    logging.info("cart_id %s: %s", cart_id, product)
                    return True

        logging.info("%s wait for %s False", cart_id, product)
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
            for (id_prd, elem) in self.products.items():
                for product_elem in elem:
                    if product == product_elem:
                        self.producers[id_prd].append(product)

            logging.info("remove %s: %s", cart_id, product)
            return True

        logging.info("failed remove %s: %s", cart_id, product)
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
            for list_products_prod in self.products.values():
                if product in list_products_prod:
                    list_products_prod.remove(product)

        logging.info("place_order:%s", list_products)
        return list_products


class TestMarketplace(unittest.TestCase):
    """
    tests
    """
    def setUp(self):
        """
        setup market
        """
        self.market_place = Marketplace(2)

    def test_add_cart(self):
        """
        new cart id
        """
        self.assertEqual(1, self.market_place.new_cart(), "Error id cart")

    def test_add_producer(self):
        """
        get producer id
        """
        self.assertEqual(1, self.market_place.register_producer(), "Error id producer")

    def test_publish(self):
        """
        publish product
        """
        id_prod = self.market_place.register_producer()
        product_add = ["prod1", ["id1", 1, 0.1], 0.2]
        self.assertEqual(True, self.market_place.publish(id_prod, product_add),
                         "Error publish")

    def test_add_to_cart(self):
        """
        add to cart
        """
        id_prod = self.market_place.register_producer()
        id_cart = self.market_place.new_cart()
        product_add = ["prod1", ["id1", 1, 0.1], 0.2]
        self.market_place.publish(id_prod, product_add)
        self.assertEqual(True, self.market_place.add_to_cart(id_cart, "prod1"),
                         "Error add_to_cart")

    def test_remove_from_cart(self):
        """
        remove from cart
        """
        id_prod = self.market_place.register_producer()
        id_cart = self.market_place.new_cart()
        product_add = ["prod1", ["id1", 1, 0.1], 0.2]
        self.market_place.publish(id_prod, product_add)
        self.market_place.add_to_cart(id_cart, "prod1")
        self.assertEqual(True, self.market_place.remove_from_cart(id_cart, "prod1"),
                         "Error remove_to_cart")

    def test_place_order(self):
        """
        check size
        """
        id_prod = self.market_place.register_producer()
        id_cart = self.market_place.new_cart()
        product_add = ["prod1", ["id1", 1, 0.1], 0.2]
        self.market_place.publish(id_prod, product_add)
        self.market_place.add_to_cart(id_cart, "prod1")
        data = self.market_place.place_order(id_cart)
        self.assertEqual(1, len(data),
                         "Error place_to_cart")
