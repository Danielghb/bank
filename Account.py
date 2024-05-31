#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
Create Account
By Daniel
At 2024-05-30 10:33:00
"""


import pymysql
con = pymysql.connect(host="localhost", user="root", passwd="root", charset="utf8", db="bank", port=3306)


def create_sql(sql: {type: str, help: "create table tb (col_name string)...;"}):
    cur = con.cursor()
    try:
        cur.execute(sql)
        con.commit()
    finally:
        cur.close()
        con.close()


def manipulate_sql(dml_sql: {type: str, help: "insert or delete or update"}):
    """
    insert into tb (col_names) values (col_vals), (col_vals)...;
    delete from tb where...;
    update tb set col_name=col_val where...;
    """
    cur = con.cursor()
    try:
        cur.execute(dml_sql)
        con.commit()
    finally:
        cur.close()
        con.close()


def save_csv(csv_path, item: {type: list}):
    with open(csv_path, 'w', encoding="utf-8", newline="\n") as csv_file:   # errors="ignore"
        for i in item:
            csv_file.write(i)


def query_sql(dql_sql: {type: str, help: "select col_name from tb...;"}):
    cur = con.cursor()
    try:
        cur.execute(dql_sql)
        data = cur.fetchall()
        # save_csv("csv_path", data)
        print(data)
    finally:
        cur.close()
        con.close()


def open_account(user_id, passwd, amount):
    cur = con.cursor()
    if amount < 500:
        exit("The account opening amount cannot be less than 500")
    try:
        cur.execute(f"insert into tb (user_id, passwd, balance) values ({user_id}, {passwd}, {amount});")
        con.commit()
    finally:
        cur.close()
        con.close()


def deposit(user_id, passwd, amount):
    cur = con.cursor()
    try:
        cur.execute(f"select balance from tb where user_id={user_id} and passwd={passwd};")
        balance = cur.fetchall()[0] + amount
        cur.execute(f"update tb set balance={balance} where user_id={user_id} and passwd={passwd};")
        con.commit()
    finally:
        cur.close()
        con.close()


def withdraw(user_id, passwd, amount):
    cur = con.cursor()
    try:
        cur.execute(f"select balance from tb where user_id={user_id} and passwd={passwd};")
        balance = cur.fetchall()[0] - amount
        if balance < 0:
            exit("Balance is insufficient")
        else:
            cur.execute(f"update tb set balance={balance} where user_id={user_id} and passwd={passwd};")
            con.commit()
    finally:
        cur.close()
        con.close()


def transfer(user_id, passwd, amount, to_user_id):
    cur = con.cursor()
    try:
        cur.execute(f"select balance from tb where user_id={user_id} and passwd={passwd};")
        balance = cur.fetchall()[0] - amount
        if balance < 0:
            exit("Balance is insufficient")
        else:
            cur.executemany(["update tb set balance=%s where user_id=%s and passwd=%s;",
                             "update tb set balance=%s+balance where user_id=%s;"],
                            [(balance, user_id, passwd), (amount, to_user_id)])
            con.commit()
    finally:
        cur.close()
        con.close()


if __name__ == '__main__':
    pass
