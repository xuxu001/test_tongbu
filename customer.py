

# -*- coding:utf-8 -*-
import pymysql
import time


class Customer(object):
    def __init__(self,conn):
        self.conn = conn

    def start_test(self):
        print(1)
        self.binding()
        print(2)
        self.binding_last()
        print(3)
        self.pay_binding()
        print(4)
        self.pay_binding_last()
        print(5)
        self.yesterday_binding_id()
        print(6)
        self.yesterday_binding()
        print(7)
        self.yesterday_binding_type()
        print(8)
        self.Settlement()
        print(9)
        self.Settlement_yesterday()
        print(10)
        self.Settlement_yesterday_error()
        print(11)
        self.Settlement_yesterday_agent_error()
        print(12)
        self.Settlement_byself()
        print(13)
        self.Settlement_agent()
        print(14)
        self.Settlement_agent_on()
        print(15)
        self.report_natura()
        print(16)
        self.report_natura_os_amount()
        print(17)
        self.report_natura_os_amount_in()
        print(18)
        self.report_natura_os_amount_na()
        print(19)
        self.report_natura_agent_amount()
        print(20)
        self.report_natura_agent_amount_in()
        print(21)
        self.report_natura_agent_amount_na()
        print(22)
        self.report_qrcode()
        print(23)
        self.report_card()
        print(24)
        self.report_card_used()
        print(25)
        self.report_card_used_report()

        # self.report_natura_agent_amount_in()


    def binding(self):
        '''拉新二维码昨日领取的数据是否存在没有拉新二维码级别的绑定'''

        try:
            cursor = self.conn.cursor()
            #所有用户的二维码
            sql = """select *
                        from t_customer a
                        join t_qrcode_receive b 
                            on a.id=b.customer_id 
                                and date(b.created_time)=DATE_SUB(CURDATE(),INTERVAL 1 DAY)
                        join t_activity d on b.activity_id=d.id and d.type=1
                        left join t_customer_agent c on a.id=c.customer_id and c.settlement_type=2
                        where c.id is null;
                        """


            data = cursor.execute(sql)
            res = cursor.fetchall()
            if res == None:
                print('绑定关系OK')
            else:
                for re in res:
                    print('绑定关系失败%s'%re[0])

        finally:
            cursor.close()

    def binding_last(self):
        '''拉新二维码昨日领取的数据对应用户的绑定时间是否为最早的一条'''

        try:
            cursor = self.conn.cursor()
            #所有用户的二维码
            sql = """select *
                    from t_customer a
                    join t_qrcode_receive b on a.id=b.customer_id
                            and date(b.created_time)=DATE_SUB(CURDATE(),INTERVAL 1 DAY)
                    join t_activity d on b.activity_id=d.id and d.type=1
                    join t_customer_agent c on a.id=c.customer_id and c.settlement_type=2
                    where b.created_time<c.bind_time
                    ;
                        """


            data = cursor.execute(sql)
            res = cursor.fetchall()
            if res == None:
                print('OK')
            else:
                for re in res:
                    print('不是最早的一条%s'%re[0])

        finally:
            cursor.close()

    def pay_binding(self):
        '''
        支付二维码昨日领取的数据是否存在没有拉新二维码级别的绑定
        '''

        try:
            cursor = self.conn.cursor()
            #所有用户的二维码
            sql = """select *
                    from t_customer a
                    join t_qrcode_receive b on a.id=b.customer_id
                            and date(b.created_time)=DATE_SUB(CURDATE(),INTERVAL 1 DAY)
                    join t_activity d on b.activity_id=d.id and d.type=2
                    left join t_customer_agent c on a.id=c.customer_id and c.settlement_type=4
                    where c.id is null
                    ;
                        """


            data = cursor.execute(sql)
            res = cursor.fetchall()
            if res == None:
                print('OK')
            else:
                for re in res:
                    print('支付不存在%s'%re[0])

        finally:
            cursor.close()

    def pay_binding_last(self):
        '''
        支付二维码昨日领取的数据对应用户的绑定时间是否为最早的一条
        '''

        try:
            cursor = self.conn.cursor()
            #所有用户的二维码
            sql = """
            select *
            from t_customer a
            join t_qrcode_receive b on a.id=b.customer_id
                    and date(b.created_time)=DATE_SUB(CURDATE(),INTERVAL 1 DAY)
            join t_activity d on b.activity_id=d.id and d.type=2
            join t_customer_agent c on a.id=c.customer_id and c.settlement_type=4
            where b.created_time<c.bind_time
            ;
                        """


            data = cursor.execute(sql)
            res = cursor.fetchall()
            if res == None:
                print('OK')
            else:
                for re in res:
                    print('支付不是最早在%s'%re[0])

        finally:
            cursor.close()

    def yesterday_binding(self):
        '''
        昨日支付数据绑定类型是否准确
        '''

        try:
            cursor = self.conn.cursor()
            #所有用户的二维码
            sql = """
            select *
            from t_payment a
            join t_customer_agent b on a.price>0 
                and date(a.payment_time)=DATE_SUB(CURDATE(),INTERVAL 1 DAY)
                and a.customer_id=b.customer_id
                and a.payment_time>b.bind_time
                and (a.settlement_type=0
                    or (a.settlement_type=1 and b.settlement_type>1)
                    or (a.settlement_type=2 and b.settlement_type in (3,4))
                )
            ; """


            data = cursor.execute(sql)
            res = cursor.fetchall()
            if res == None:
                print('OK')
            else:
                for re in res:
                    print('昨日支付%s'%re[0])

        finally:
            cursor.close()

    def yesterday_binding_id(self):
        '''
        判断支付中绑定了之后合伙人ID不一致的问题
        '''

        try:
            cursor = self.conn.cursor()
            sql = """
             select a.*,b.customer_id,c.*
                from t_payment a
                join t_customer_agent b on a.customer_agent_id=b.id and a.agent_id!=b.agent_id
                    and date(a.payment_time)=DATE_SUB(CURDATE(),INTERVAL 1 DAY)
                join t_customer c on a.customer_id=c.id
                ;       
               """

            data = cursor.execute(sql)
            res = cursor.fetchall()
            if res == None:
                print('OK')
            else:
                for re in res:
                    print('昨日绑定id%s' % re[0])

        finally:
            cursor.close()

    def yesterday_binding_type(self):
        '''
        判断支付中结算类型是否和绑定数据中结算类型一致
        '''

        try:
            cursor = self.conn.cursor()
            sql = """
            select *
                from t_payment a
                join t_customer_agent b on a.customer_agent_id=b.id and a.settlement_type!=b.settlement_type
                    and date(a.payment_time)=DATE_SUB(CURDATE(),INTERVAL 1 DAY)
                ;    
               """

            data = cursor.execute(sql)
            res = cursor.fetchall()
            if res == None:
                print('OK')
            else:
                for re in res:
                    print('昨日绑定类型id%s' % re[0])

        finally:
            cursor.close()

    def Settlement(self):
        '''
        判断结算表中和支付表中合伙人不一致的数据

        '''

        try:
            cursor = self.conn.cursor()
            sql = """
            select *
                from t_payment_settlement a
                join t_payment b on a.payment_id=b.id and a.settlement_type=b.settlement_type
                    and date(b.payment_time)=DATE_SUB(CURDATE(),INTERVAL 1 DAY)
                join t_agent c on b.agent_id=c.id
                    and ((c.parent_id is null and a.sub_flag=0) or (c.parent_id is not null and a.sub_flag=1))
                where a.agent_id!=b.agent_id 
                ;
   
               """

            data = cursor.execute(sql)
            res = cursor.fetchall()
            if res == None:
                print('OK')
            else:
                for re in res:
                    print('结算id%s' % re[0])

        finally:
            cursor.close()

    def Settlement_yesterday(self):
        '''
        判断昨日结算中是否有金额与支付数据不一致的

        '''

        try:
            cursor = self.conn.cursor()
            sql = """
            select *
                from t_payment a
                join t_payment_settlement b on a.id=b.payment_id
                    and date(a.payment_time)=DATE_SUB(CURDATE(),INTERVAL 1 DAY)
                where a.price!=b.amount
                ;

               """

            data = cursor.execute(sql)
            res = cursor.fetchall()
            if res == None:
                print('OK')
            else:
                for re in res:
                    print('结算昨日id%s' % re[0])

        finally:
            cursor.close()

    def Settlement_yesterday_error(self):
        '''
             判断昨日结算中是否有平台结算后金额错误数据
        '''

        try:
            cursor = self.conn.cursor()
            sql = """
            select *
                from t_payment_settlement a
                join t_payment_settlement_config b on a.settlement_type=b.settlement_type and a.sub_flag=0
                join t_payment c on a.payment_id=c.id
                    and date(c.payment_time)=DATE_SUB(CURDATE(),INTERVAL 1 DAY)
                where a.os_amount!=if(c.os_type=1,a.amount*b.android_percent,a.amount*ios_percent)
                ;

               """

            data = cursor.execute(sql)
            res = cursor.fetchall()
            if res == None:
                print('OK')
            else:
                for re in res:
                    print('结算昨日错误id%s' % re[0])

        finally:
            cursor.close()

    def Settlement_yesterday_agent_error(self):
        '''
             判断昨日结算中是否有合伙人结算金额错误数据
        '''

        try:
            cursor = self.conn.cursor()
            sql = """
            select *
                from t_payment_settlement a
                join t_payment_settlement_config b on a.settlement_type=b.settlement_type and a.sub_flag=0
                where a.agent_amount-a.os_amount*b.agent_percent>1
                    or a.agent_amount-a.os_amount*b.agent_percent<-1
;

                """

            data = cursor.execute(sql)
            res = cursor.fetchall()
            if res == None:
                print('OK')
            else:
                for re in res:
                    print('结算昨日错误合伙人id%s' % re[0])

        finally:
            cursor.close()

    def Settlement_byself(self):
        '''
             本身的结算
        '''

        try:
            cursor = self.conn.cursor()
            sql = """
               select *
                    from t_payment a
                    join t_payment_settlement_config b on a.settlement_type=b.settlement_type
                        and date(a.payment_time)=DATE_SUB(CURDATE(),INTERVAL 1 DAY)
                    left join t_payment_settlement c on c.payment_id=a.id
                    where c.id is null and a.price>0 and a.agent_id>0
                    ;

                    """

            data = cursor.execute(sql)
            res = cursor.fetchall()
            if res == None:
                print('OK')
            else:
                for re in res:
                    print('本身的结算id%s' % re[0])

        finally:
            cursor.close()

    def Settlement_agent(self):
        '''
             如果有上级则对上级结算
        '''

        try:
            cursor = self.conn.cursor()
            sql = """
            select *
                from t_payment a
                join t_payment_settlement_config b on a.settlement_type=b.settlement_type
                    and date(a.payment_time)=DATE_SUB(CURDATE(),INTERVAL 1 DAY)
                join t_agent d on a.agent_id=d.id and d.parent_id is not null
                left join t_payment_settlement c on c.payment_id=a.id and c.agent_id=d.parent_id
                where c.id is null and a.price>0
                ;

                    """

            data = cursor.execute(sql)
            res = cursor.fetchall()
            if res == None:
                print('OK')
            else:
                for re in res:
                    print('上级的结算id%s' % re[0])

        finally:
            cursor.close()

    def Settlement_agent_on(self):
        '''
             如果是内部合伙人则有内部引流自然流量结算
        '''

        try:
            cursor = self.conn.cursor()
            sql = """
            select *
                from t_agent a
                join t_payment b on a.id=b.agent_id
                    and date(b.payment_time)=DATE_SUB(CURDATE(),INTERVAL 1 DAY)
                join t_customer_agent c on b.customer_id=c.customer_id and c.settlement_type=1
                join t_payment_settlement_config d on d.settlement_type=1
                left join t_payment_settlement e on e.payment_id=b.id and e.settlement_type=1
                where a.type=2 and b.price>0 and b.source_type=1 and e.id is null
                ;

                    """

            data = cursor.execute(sql)
            res = cursor.fetchall()
            if res == None:
                print('OK')
            else:
                for re in res:
                    print('上级的结算id%s' % re[0])

        finally:
            cursor.close()

    def report_natura(self):
        '''
             自然报表中代理金额和总金额是否符合
        '''

        try:
            cursor = self.conn.cursor()
            sql = """
           select *
                from t_report_nature_daily
                where agent_amount - os_amount/10 > 1 or agent_amount - os_amount/10 < -1
                ;

                    """

            data = cursor.execute(sql)
            res = cursor.fetchall()
            if res == None:
                print('OK')
            else:
                for re in res:
                    print('自然流量结算id%s' % re[0])

        finally:
            cursor.close()

    def report_natura_os_amount(self):
        '''
             自然流量中的os_amount
        '''

        try:
            cursor = self.conn.cursor()
            sql = """
           select a.id,sum(b.os_amount) as os_amount1,a.os_amount
                from t_report_nature_daily a
                join t_payment_settlement b on a.agent_id=b.agent_id and b.settlement_type=1 and b.sub_flag=0
                join t_payment c on b.payment_id=c.id and a.os_type=c.os_type and a.report_date=date(c.payment_time)
                where a.report_date=DATE_SUB(CURDATE(),INTERVAL 1 DAY)
                group by a.id
                having a.os_amount!=sum(b.os_amount)
                ;

                     """

            data = cursor.execute(sql)
            res = cursor.fetchall()
            if res == None:
                print('OK')
            else:
                for re in res:
                    print('自然流量os_amount结算id%s' % re[0])

        finally:
            cursor.close()

    def report_natura_os_amount_na(self):
        '''
             自然流量中的os_amount_na
        '''

        try:
            cursor = self.conn.cursor()
            sql = """
                select a.id,sum(b.os_amount) as os_amount1,a.os_amount_na
                    from t_report_nature_daily a
                    join t_payment_settlement b on a.agent_id=b.agent_id and b.settlement_type=1 and b.sub_flag=0 and b.cus_source_type=1
                    join t_payment c on b.payment_id=c.id and a.os_type=c.os_type and a.report_date=date(c.payment_time)
                    where a.report_date=DATE_SUB(CURDATE(),INTERVAL 1 DAY)
                    group by a.id
                    having a.os_amount_na!=sum(b.os_amount)
                    ;

                     """

            data = cursor.execute(sql)
            res = cursor.fetchall()
            if res == None:
                print('OK')
            else:
                for re in res:
                    print('自然流量os_amount_na结算id%s' % re[0])

        finally:
            cursor.close()

    def report_natura_os_amount_in(self):
        '''
             自然流量中的os_amount_in
        '''

        try:
            cursor = self.conn.cursor()
            sql = """
                select a.id,sum(b.os_amount) as os_amount1,a.os_amount_in
                    from t_report_nature_daily a
                    join t_payment_settlement b on a.agent_id=b.agent_id and b.settlement_type=1 and b.sub_flag=0 and b.cus_source_type=2
                    join t_payment c on b.payment_id=c.id and a.os_type=c.os_type and a.report_date=date(c.payment_time)
                    where a.report_date=DATE_SUB(CURDATE(),INTERVAL 1 DAY)
                    group by a.id
                    having a.os_amount_in!=sum(b.os_amount)
                    ;

                     """

            data = cursor.execute(sql)
            res = cursor.fetchall()
            if res == None:
                print('OK')
            else:
                for re in res:
                    print('自然流量os_amount_in结算id%s' % re[0])

        finally:
            cursor.close()

    def report_natura_agent_amount(self):
        '''
            自然流量中的agent_amount

        '''

        try:
            cursor = self.conn.cursor()
            sql = """
              select a.id,sum(b.agent_amount) as agent_amount1,a.agent_amount,a.report_date
                    from t_report_nature_daily a
                    join t_payment_settlement b on a.agent_id=b.agent_id and b.settlement_type=1 and b.sub_flag=0
                    join t_payment c on b.payment_id=c.id and a.os_type=c.os_type and a.report_date=date(c.payment_time)
                    where a.report_date=DATE_SUB(CURDATE(),INTERVAL 1 DAY)
                    group by a.id
                    having a.agent_amount!=agent_amount1
                    ;

                     """

            data = cursor.execute(sql)
            res = cursor.fetchall()
            if res == None:
                print('OK')
            else:
                for re in res:
                    print('自然流量agent_amount结算id%s' % re[0])

        finally:
            cursor.close()

    def report_natura_agent_amount_na(self):
        '''自然流量中的agent_amount_na'''

        try:
            cursor = self.conn.cursor()
            sql = """
                select a.id,sum(b.agent_amount) as agent_amount_na1,a.agent_amount_na,a.report_date
                    from t_report_nature_daily a
                    join t_payment_settlement b on a.agent_id=b.agent_id and b.settlement_type=1 and b.sub_flag=0 and b.cus_source_type=1
                    join t_payment c on b.payment_id=c.id and a.os_type=c.os_type and a.report_date=date(c.payment_time)
                    where a.report_date=DATE_SUB(CURDATE(),INTERVAL 1 DAY)
                    group by a.id
                    having a.agent_amount_na!=agent_amount_na1
                    ;
                  """

            data = cursor.execute(sql)
            res = cursor.fetchall()
            if res == None:
                print('OK')
            else:
                for re in res:
                    print('自然流量agent_amount_na结算id%s' % re[0])

        finally:
            cursor.close()

    def report_qrcode(self):
        '''
            二维码的os_amount或者agent_amount

        '''

        try:
            cursor = self.conn.cursor()
            sql = """
                select a.id,a.os_amount,a.agent_amount
                            ,sum(if(source_type=1,c.os_amount,0)) as os_amount1
                            ,sum(if(source_type=1,c.agent_amount,0)) as agent_amount1
                    from t_report_qrcode_daily a
                    join t_payment b 
                        on a.os_type=b.os_type and b.payment_time like concat(a.report_date, '%')
                            and a.report_date=DATE_SUB(CURDATE(),INTERVAL 1 DAY)
                    join t_payment_settlement c
                    on b.id=c.payment_id and c.settlement_type in (2,4) and a.qrcode_id=c.settlement_detail_id and sub_flag=0
                    group by a.id
                    having a.os_amount!=os_amount1 or a.agent_amount!=agent_amount1
                    ;
                     """

            data = cursor.execute(sql)
            res = cursor.fetchall()
            if res == None:
                print('OK')
            else:
                for re in res:
                    print('二维码结算id%s' % re[0])

        finally:
            cursor.close()


    def report_card(self):
        '''
            实体卡的os_amount或者agent_amount

        '''

        try:
            cursor = self.conn.cursor()
            sql = """
               select a.id,a.os_amount,a.agent_amount
                    ,sum(c.os_amount) as os_amount1
                    ,sum(c.agent_amount) as agent_amount1
                    from t_report_card_daily a
                    join t_payment b 
                    on b.settlement_type=3 and a.card_type_id=b.settlement_detail_id and a.agent_id=b.agent_id
                            and b.payment_time like concat(a.report_date, '%') 
                            and a.report_date=DATE_SUB(CURDATE(),INTERVAL 1 DAY)
                    join t_payment_settlement c
                    on b.id=c.payment_id and c.settlement_type=b.settlement_type
                    group by a.id
                    having a.os_amount!=sum(c.os_amount) or a.agent_amount!=sum(c.agent_amount)
                    ;
                     """

            data = cursor.execute(sql)
            res = cursor.fetchall()
            if res == None:
                print('OK')
            else:
                for re in res:
                    print('实体卡结算id%s' % re[0])

        finally:
            cursor.close()

    def report_card_used(self):
        '''
            实体卡的使用数量是否准确

        '''

        try:
            cursor = self.conn.cursor()
            sql = """
                select a.*,count(b.id)
                    from t_report_card_daily a
                    join (select *
                    from t_card
                    where used_flag=1 and date(used_time)=DATE_SUB(CURDATE(),INTERVAL 1 DAY)) b
                        on (a.agent_id=b.agent_id or a.agent_id=b.parent_id) and a.card_type_id=b.card_type_id and a.report_date=date(b.used_time)
                    group by a.id
                    having a.used_number!=count(b.id)
                    ;
                     """

            data = cursor.execute(sql)
            res = cursor.fetchall()
            if res == None:
                print('OK')
            else:
                for re in res:
                    print('实体卡使用数量id%s' % re[0])

        finally:
            cursor.close()


    def report_card_used_report(self):
        '''
            有使用的实体卡但是没有日报表数据

        '''

        try:
            cursor = self.conn.cursor()
            sql = """
                 select *
                    from (select *
                    from t_card
                    where used_flag=1 and agent_id>0
                    and date(used_time)=DATE_SUB(CURDATE(),INTERVAL 1 DAY)) a
                    left join t_report_card_daily b 
                        on a.agent_id=b.agent_id 
                            and date(a.used_time)=b.report_date
                            and a.card_type_id=b.card_type_id
                    where b.id is null
                    ;
                          """

            data = cursor.execute(sql)
            res = cursor.fetchall()
            if res == None:
                print('OK')
            else:
                for re in res:
                    print('实体卡使用无报表id%s' % re[0])

        finally:
            cursor.close()

    def report_natura_agent_amount_in(self):
        '''
            自然流量中的agent_amount_in

        '''

        try:
            cursor = self.conn.cursor()
            sql = """
                select a.id,sum(b.agent_amount) as agent_amount_in1,a.agent_amount_in,a.report_date
                    from t_report_nature_daily a
                    join t_payment_settlement b on a.agent_id=b.agent_id and b.settlement_type=1 and b.sub_flag=0 and b.cus_source_type=2
                    join t_payment c on b.payment_id=c.id and a.os_type=c.os_type and a.report_date=date(c.payment_time)
                    where a.report_date=DATE_SUB(CURDATE(),INTERVAL 1 DAY)
                    group by a.id
                    having a.agent_amount_in!=agent_amount_in1
                    ;

                     """

            data = cursor.execute(sql)
            res = cursor.fetchall()
            if res == None:
                print('OK')
            else:
                for re in res:
                    print('自然流量agent_amount_in结算id%s' % re[0])

        finally:
            cursor.close()



if __name__ == "__main__":
    conn = pymysql.connect(
        host="rm-bp1349z8ay2u070sfmo.mysql.rds.aliyuncs.com",
        user="read_only",
        password="7dian7fen_read_only",
        db="wxb-agent",
        charset='utf8',
        port=3306
    )
    test = Customer(conn)
    test_customer = test.start_test()
    conn.close()

