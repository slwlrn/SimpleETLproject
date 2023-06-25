{
 "cells": [
  {
   "cell_type": "code",
   "execution_count": 1,
   "id": "852bd389-b09e-4fb5-be72-e7c77e83e4ed",
   "metadata": {},
   "outputs": [],
   "source": [
    "\n",
    "from process.Extract import Extract\n",
    "from process.Load import Load\n",
    "from process.Transform import Transform\n",
    "import pandas as pd\n",
    "from utils import utilitarios as u\n",
    "extract = Extract()\n",
    "load = Load()\n",
    "transform =Transform()"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 2,
   "id": "31fb811d-4e99-4811-ad83-96dba871a8e4",
   "metadata": {},
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "Error al leer desde MySQL: (MySQLdb.OperationalError) (2002, \"Can't connect to MySQL server on '172.17.48.1' (115)\")\n",
      "(Background on this error at: https://sqlalche.me/e/20/e3q8)\n",
      "Error al leer desde MySQL: (MySQLdb.OperationalError) (2002, \"Can't connect to MySQL server on '172.17.48.1' (115)\")\n",
      "(Background on this error at: https://sqlalche.me/e/20/e3q8)\n",
      "Error al leer desde MySQL: (MySQLdb.OperationalError) (2002, \"Can't connect to MySQL server on '172.17.48.1' (115)\")\n",
      "(Background on this error at: https://sqlalche.me/e/20/e3q8)\n"
     ]
    }
   ],
   "source": [
    "customers_df = extract.read_mysql('customers','retail_db')\n",
    "orders_df = extract.read_mysql('orders','retail_db')\n",
    "order_items_df = extract.read_mysql('order_items','retail_db')\n",
    "# Read products\n",
    "products_df = extract.read_mongoatlas('retail_db', \"products\")\n",
    "departments_df = extract.read_mongoatlas('retail_db', \"departments\")\n",
    "categories_df = extract.read_mongoatlas('retail_db', \"categories\")"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "id": "d25e7d76-2540-4d3d-b301-4a68640c119f",
   "metadata": {},
   "outputs": [],
   "source": [
    "#load\n",
    "load.load_to_s3(customers_df, \"ingestraw\", \"retail/customers\")\n",
    "load.load_to_s3(orders_df, \"ingestraw\", \"retail/orders\")\n",
    "load.load_to_s3(order_items_df, \"ingestraw\", \"retail/order_items\")\n",
    "load.load_to_s3(products_df, \"ingestraw\", \"retail/products\")\n",
    "load.load_to_s3(departments_df, \"ingestraw\", \"retail/departments\")\n",
    "load.load_to_s3(categories_df, \"ingestraw\", \"retail/categories\")"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "id": "6803a524-68f5-497f-8179-927884399a65",
   "metadata": {},
   "outputs": [],
   "source": []
  }
 ],
 "metadata": {
  "kernelspec": {
   "display_name": "Python 3 (ipykernel)",
   "language": "python",
   "name": "python3"
  },
  "language_info": {
   "codemirror_mode": {
    "name": "ipython",
    "version": 3
   },
   "file_extension": ".py",
   "mimetype": "text/x-python",
   "name": "python",
   "nbconvert_exporter": "python",
   "pygments_lexer": "ipython3",
   "version": "3.9.16"
  }
 },
 "nbformat": 4,
 "nbformat_minor": 5
}
