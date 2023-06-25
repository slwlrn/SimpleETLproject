{
 "cells": [
  {
   "cell_type": "code",
   "execution_count": 1,
   "id": "a278191b-0263-4741-9ea0-36070bcff842",
   "metadata": {},
   "outputs": [],
   "source": [
    "from process.Extract import Extract\n",
    "from process.Transform import Transform\n",
    "from process.Load import Load\n",
    "import pandas as pd\n"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 2,
   "id": "00ef2b10-311f-484b-90be-c1fd0114c5bf",
   "metadata": {},
   "outputs": [],
   "source": [
    "extract = Extract()\n",
    "transform = Transform()\n",
    "load = Load()\n"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 7,
   "id": "893fca25-9538-4028-9f4b-2f415392493c",
   "metadata": {},
   "outputs": [],
   "source": [
    "# Read from S3\n",
    "customers_df = extract.read_s3('ingestraw', 'retail/customers.csv')\n",
    "orders_df = extract.read_s3('ingestraw', 'retail/orders.csv')\n",
    "order_items_df = extract.read_s3('ingestraw', 'retail/order_items.csv')\n",
    "products_df = extract.read_s3('ingestraw', 'retail/products.csv')\n",
    "departments_df = extract.read_s3('ingestraw', 'retail/departments.csv')\n",
    "categories_df = extract.read_s3('ingestraw', 'retail/categories.csv')"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "id": "2ccffe73-012d-4333-bd44-1a7520ce0f0e",
   "metadata": {
    "scrolled": true
   },
   "outputs": [],
   "source": [
    "df_enunciado1 = transform.enunciado1(customers_df,orders_df, order_items_df)\n",
    "df_enunciado2 = transform.enunciado2(order_items_df, products_df,categories_df)"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 10,
   "id": "779698a0-00cd-4e3f-9d5c-992a61d31eb4",
   "metadata": {},
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "Carga a S3 exitosa\n",
      "Carga a S3 exitosa\n"
     ]
    }
   ],
   "source": [
    "load.load_to_s3(df_enunciado1,\"ingestraw\", \"gold/df_enunciado1\")\n",
    "load.load_to_s3(df_enunciado2,\"ingestraw\", \"gold/df_enunciado2\")"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "id": "c1ea3cdb-da39-4f12-82b6-05fa0d3470cd",
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
