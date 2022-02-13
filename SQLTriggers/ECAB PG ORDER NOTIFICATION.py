
#SQL Trigger and Table creation, python pulse check follows. 




# CREATE TRIGGER NotifyPandGOrders on [oe_hdr]
# FOR INSERT 
# AS DECLARE @Taker VARCHAR(25),
# 	   @OrderNo CHAR(10),
# 	   @CustomerID decimal(19,0),
# 	   @CustomerName VARCHAR(50),
# 	   @OrderDate datetime,
# 	   @RequestedDate datetime,
# 	   @Approved CHAR(1) ;

# SELECT @Taker = ins.taker FROM INSERTED ins;
# SELECT @OrderNo = ins.order_no FROM INSERTED ins;
# SELECT @CustomerID = ins.customer_id FROM INSERTED ins;
# SELECT @CustomerName = customer.customer_name FROM INSERTED ins INNER JOIN customer ON (customer.customer_id = ins.customer_id);
# SELECT @OrderDate = ins.order_date FROM INSERTED ins;
# SELECT @RequestedDate = ins.requested_date FROM INSERTED ins;
# SELECT @Approved = ins.approved FROM INSERTED ins;

# IF @Approved = 'Y' 
# AND @CustomerID IN (100523, 100927, 100928, 100929, 100930, 100933, 100935, 100937, 100939, 100940, 100941, 100942, 100944, 100945, 100946, 100949, 100950, 108588, 109068, 109717, 109743, 109982, 110222, 112346, 113111, 113228, 113239, 113269, 113310, 113342, 113498, 113499,113531, 113651, 114262, 114425, 114612, 114821, 114830, 114875, 114962, 114965, 114993, 114996, 115059, 115149, 115648, 116021, 117198, 117655, 117741, 119131, 119142, 120564, 121680, 121708, 124716, 124847, 124903, 124907, 124909, 124913, 124936, 125422, 125669, 125802, 127634, 127679) 
# AND @Taker = 'ECABAK'

# 	BEGIN

# 		INSERT INTO [cinci_pandg_orders]( 
# 		[Order Taker]
# 		,[Order No.]
# 		,[Customer ID]
# 		,[Customer Name]
# 		,[Order Date]
# 		,[RequestedDate],
# 		[Approved],
# 		[Notified])
# 		VALUES (@Taker,
# 		@OrderNo,
# 		@CustomerID,
# 		@CustomerName,
# 		@OrderDate,
# 		@RequestedDate,
# 		@Approved,
# 		'N');
# 	END
# 	ELSE
# 	BEGIN
# 		return
# 	END

# 	USE [DATABASE]
# GO

# SET ANSI_NULLS ON
# GO

# SET QUOTED_IDENTIFIER ON
# GO

# SET ANSI_PADDING ON
# GO

# CREATE TABLE [dbo].[cinci_pandg_orders](
# 		[Order Taker] VARCHAR(25),
# 		[Order No.] decimal(19,0),
# 		[Customer ID] decimal(19,0),
# 		[Customer Name] VARCHAR(40),
# 		[Order Date] datetime,
# 		[RequestedDate] datetime,
# 		[Approved] CHAR(1),
# 		[Notified] CHAR(1)
# 			);

# GO

# SET ANSI_PADDING OFF
# GO























import sqlalchemy as sa
import pandas as pd
from datetime import datetime, timedelta
import smtplib
import time
from email.message import EmailMessage
import ctypes
engine = sa.create_engine("mssql+pymssql://username:password@Server/Database", echo=True)



def readTable():
    SelectQ = '''SELECT * from cinci_pandg_orders;'''
    erincincidata = pd.read_sql_query(SelectQ, engine)
    cincipandgtable = pd.DataFrame(erincincidata)
    cincipandgtable = cincipandgtable.loc[cincipandgtable['Notified'] == 'N']
    cincipandgtable['Customer ID'] = cincipandgtable['Customer ID'].astype(int)
    cincipandgtable['Order No.'] = cincipandgtable['Order No.'].astype(int)
    cincipandgtable['Order Date'] = pd.to_datetime(cincipandgtable['Order Date'], format='%m/%d/%Y')
    cincipandgtable['RequestedDate'] = pd.to_datetime(cincipandgtable['RequestedDate'], format='%m/%d/%Y')
    return cincipandgtable


def UpdateSQL(OrderNo):
    UpdateQuery = f'''UPDATE cinci_pandg_orders
    SET [Notified] = 'Y'
    FROM cinci_pandg_orders
    WHERE [Order No.] = {OrderNo} ;
    '''
    engine.execute(UpdateQuery)



sender = 'sqlagent@e-mail.com'
receiver = 'me@mye-mail.com'

SMTPserver  = 'SMTP.Server.Address'



while 1:
    cincipandgtable = readTable()
    print(cincipandgtable)
    if len(cincipandgtable) > 0:
        for index, column in cincipandgtable.iterrows():

            msg = EmailMessage()

            msg.add_header('Content-Type','text/html')

            content = f'''
        <!DOCTYPE html PUBLIC "-//W3C//DTD XHTML 1.0 Transitional//EN" http://www.w3.org/TR/xhtml1/DTD/xhtml1-transitional.dtd><html xmlns=http://www.w3.org/1999/xhtml>
            <head>
                <meta http-equiv="Content-Type" content="text/html; charset=utf-8" />
                <meta http-equiv="X-UA-Compatible" content="IE=edge" />
                <meta name="viewport" content="width=device-width, initial-scale=1.0">
                <title></title>
            </head>

            <body>
                <center class="wrapper"
                    style="width: 100%; table-layout: fixed; -webkit-text-size-adjust: 100%; -ms-text-size-adjust: 100%;">
                    <div class="webkit"
                        style="max-width: 600px; margin: 0 auto; border-style: ridge; border: 3px; border-color: #336699; border-radius: 5px;">
                        <table class="outer" align="center"
                            style="border-spacing: 0; font-family: sans-serif; color: #333333; Margin: 0 auto; width: 100%; max-width: 600px; border: 4px; border-color: #336699; border-style: ridge;">
                            <tr>
                            </tr>
                            <td><table  width="100%"></br>
                        <p align="center" style="Margin: 0; font-size: 18px; Margin-bottom: 5px;">NEW P&G ORDER ENTERED</p></br>
                    </div>
                    </td>
                    </tr>
                    </td>
                    </tr>
                    </td>
                    </tr>
                    <tr>
                        <td>
                            <table role="presentation" width="100%"
                                style="border-collapse:collapse;mso-table-lspace:0pt;mso-table-rspace:0pt;border-spacing:0;font-family:verdana, arial, helvetica, sans-serif;color:#000;">
                                <tr>
                                    <td width="74" align="center" valign="center" bgcolor="#1f557b"
                                        style="text-align: center; padding-top:5px;padding-bottom:5px;padding-left:0;padding-right:0;">
                                        <span
                                            style="color: #FFFFFF; font-family: verdana;font-weight: bold; font-size: 12px; ">&nbsp;
                                            Description</span></td>
                                    <td width="181" align="center" valign="center" bgcolor="#1f557b" class="inner2 contents2"
                                        style="text-align: center; padding-top:5px;padding-bottom:5px;padding-left:0;padding-right:0;">
                                        <span
                                            style="color:#FFFFFF;font-family: verdana;font-weight: bold; font-size: 12px;">Order Info</span>
                                    </td>
                                </tr>

                                <tr>
                                    <td bgcolor="#FFFFFF" class="inner2 contents1"
                                        style="border:ridge;border-collapse:collapse;text-align:Right;padding-right:5px;padding-left:5px;padding-top:5px;padding-bottom:0px; font-family: verdana;font-size: 12px;">
                                        <span style="font-size:12px;">
                                            Order #: 
                                        </span></td>
                                    <td bgcolor="#FFFFFF" class="inner2 contents1"
                                        style="border:ridge;border-collapse:collapse;text-align:Left;padding-right:5px;padding-left:5px;padding-top:5px;padding-bottom:0px;font-family: verdana;">
                                        <span style="font-size:12px;">
                                            {cincipandgtable.loc[index, 'Order No.']}
                                        </span></td>
                                </tr>

                                <tr>
                                    <td bgcolor="#FFFFFF" class="inner2 contents1"
                                        style="border:ridge;border-collapse:collapse;text-align:Right;padding-right:5px;padding-left:5px;padding-top:5px;padding-bottom:0px; font-family: verdana;font-size: 12px;">
                                        <span style="font-size:12px;">
                                            Order Taker:
                                        </span></td>
                                    <td bgcolor="#FFFFFF" class="inner2 contents1"
                                        style="border:ridge;border-collapse:collapse;text-align:Left;padding-right:5px;padding-left:5px;padding-top:5px;padding-bottom:0px;font-family: verdana;">
                                        <span style="font-size:12px;">
                                            {cincipandgtable.loc[index, 'Order Taker']}
                                        </span></td>
                                </tr>
                                <tr>
                                    <td bgcolor="#FFFFFF" class="inner2 contents1"
                                        style="border:ridge;border-collapse:collapse;text-align:Right;padding-right:5px;padding-left:5px;padding-top:5px;padding-bottom:0px; font-family: verdana;font-size: 12px;">
                                        <span style="font-size:12px;">
                                            Customer ID:
                                        </span></td>
                                    <td bgcolor="#FFFFFF" class="inner2 contents1"
                                        style="border:ridge;border-collapse:collapse;text-align:Left;padding-right:5px;padding-left:5px;padding-top:5px;padding-bottom:0px;font-family: verdana;">
                                        <span style="font-size:12px;">
                                            {cincipandgtable.loc[index, 'Customer ID']}
                                        </span></td>
                                </tr>

                                <tr>
                                    <td bgcolor="#FFFFFF" class="inner2 contents1"
                                        style="border:ridge;border-collapse:collapse;text-align:Right;padding-right:5px;padding-left:5px;padding-top:5px;padding-bottom:0px; font-family: verdana;font-size: 12px;">
                                        <span style="font-size:12px;">
                                            Customer Name:
                                        </span></td>
                                    <td bgcolor="#FFFFFF" class="inner2 contents1"
                                        style="border:ridge;border-collapse:collapse;text-align:Left;padding-right:5px;padding-left:5px;padding-top:5px;padding-bottom:0px;font-family: verdana;">
                                        <span style="font-size:12px;">
                                            {cincipandgtable.loc[index, 'Customer Name']}
                                        </span></td>
                                </tr>

                                <tr>
                                    <td bgcolor="#FFFFFF" class="inner2 contents1"
                                        style="border:ridge;border-collapse:collapse;text-align:Right;padding-right:5px;padding-left:5px;padding-top:5px;padding-bottom:0px; font-family: verdana;font-size: 12px;">
                                        <span style="font-size:12px;">
                                            Order Date:
                                        </span></td>
                                    <td bgcolor="#FFFFFF" class="inner2 contents1"
                                        style="border:ridge;border-collapse:collapse;text-align:Left;padding-right:5px;padding-left:5px;padding-top:5px;padding-bottom:0px;font-family: verdana;">
                                        <span style="font-size:12px;">
                                            {cincipandgtable.loc[index, 'Order Date']}
                                        </span></td>
                                </tr>
                                <tr>
                                    <td bgcolor="#FFFFFF" class="inner2 contents1"
                                        style="border:ridge;border-collapse:collapse;text-align:Right;padding-right:5px;padding-left:5px;padding-top:5px;padding-bottom:0px; font-family: verdana;font-size: 12px;">
                                        <span style="font-size:12px;">
                                            Requested Date:
                                        </span></td>
                                    <td bgcolor="#FFFFFF" class="inner2 contents1"
                                        style="border:ridge;border-collapse:collapse;text-align:Left;padding-right:5px;padding-left:5px;padding-top:5px;padding-bottom:0px;font-family: verdana;">
                                        <span style="font-size:12px;">
                                            {cincipandgtable.loc[index, 'RequestedDate']}
                                        </span></td>
                                </tr>
                        </td>
                    </tr>
                </table>
                    </table>
                    <tr>
                        <td bgcolor="#FFFFFF" class="inner2 contents1" style="text-align: right; font-size: 13;">
                        </td>
                        </div>
                        <!--[if (gte mso 9)|(IE)]> </td></tr></table><![endif]-->
                        </td>
                    </tr>
                    </table>
                    <!--[if (gte mso 9)|(IE)]> </td></tr></table><![endif]-->
                    </div>
                </center>
            </body>
            </html>
        '''

            msg.set_content(content,subtype='html')
            msg['Subject'] = f"ECABAK has just entered P&G Order# {cincipandgtable.loc[index, 'Order No.']}"
            msg['From'] = sender
            msg['To'] = receiver
            try:
                with smtplib.SMTP(SMTPserver) as smtpObj:
                    smtpObj.send_message(msg)
                UpdateSQL(cincipandgtable.loc[index, 'Order No.'])
            except Exception as e:
                print("Error: unable to send email")
                print(e)
                ctypes.windll.user32.MessageBoxW(0, f"{cincipandgtable.loc[index,'Order Taker']} was not notified about Order No. {cincipandgtable.loc[index, 'Order No.']}", "Failed to Send E-mail", 0x40000)
    else:
        pass

    time.sleep(900)


