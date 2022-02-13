
#SQL Table and Trigger creation, python pulse check follows. 


# CREATE TRIGGER ReqEqOrderDate on [oe_hdr]
# FOR INSERT 
# AS DECLARE @Taker VARCHAR(25),
# 	   @OrderNo CHAR(10),
# 	   @CustomerID decimal(19,0),
# 	   @CustomerName VARCHAR(50),
# 	   @SalesLocation decimal(19,0), 
# 	   @OrderDate datetime,
# 	   @RequestedDate datetime,
# 	   @Approved CHAR(1),
# 	   @RMA_flag CHAR(1),
#        @Quote CHAR(1) ;


# SELECT @Taker = ins.taker FROM INSERTED ins;
# SELECT @OrderNo = ins.order_no FROM INSERTED ins;
# SELECT @CustomerID = ins.customer_id FROM INSERTED ins;
# SELECT @CustomerName = customer.customer_name FROM INSERTED ins INNER JOIN customer ON (customer.customer_id = ins.customer_id);
# SELECT @SalesLocation = ins.location_id FROM INSERTED ins;
# SELECT @OrderDate = ins.order_date FROM INSERTED ins;
# SELECT @RequestedDate = ins.requested_date FROM INSERTED ins;
# SELECT @Approved = ins.approved FROM INSERTED ins;
# SELECT @Quote = ins.projected_order FROM INSERTED ins;
# SELECT @RMA_flag = ins.rma_flag FROM INSERTED ins; 

# IF @RequestedDate <= @OrderDate AND @Approved = 'Y' AND @RMA_flag = 'N' AND @Quote = 'N' AND @SalesLocation NOT IN (120439, 107390) AND @Taker NOT IN ('ADMIN', 'EDI_ORDER') AND @CustomerID <> 121359
# 	BEGIN

# 		INSERT INTO [requested_date_audit_table]( 
# 		[Order Taker]
# 		,[Order No.]
# 		,[Customer ID]
# 		,[Customer Name]
# 		,[Sales Location]
# 		,[Order Date]
# 		,[RequestedDate],
# 		[Notified])
# 		VALUES (@Taker,
# 		@OrderNo,
# 		@CustomerID,
# 		@CustomerName,
# 		@SalesLocation,
# 		@OrderDate,
# 		@RequestedDate,
# 		'N');
# 	END
# 	ELSE
# 	BEGIN
# 		return
# 	END



# USE [CommerceCenter]
# GO

# SET ANSI_NULLS ON
# GO

# SET QUOTED_IDENTIFIER ON
# GO

# SET ANSI_PADDING ON
# GO

# CREATE TABLE [dbo].[requested_date_audit_table](
# 	[Order Taker] VARCHAR(25),
# 		[Order No.] decimal(19,0),
# 		[Customer ID] decimal(19,0),
# 		[Customer Name] VARCHAR(40),
# 		[Sales Location] decimal(19,0),
# 		[Order Date] datetime,
# 		[RequestedDate] datetime,
# 		[Notified] CHAR(1)

# 		);

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


def DoubleCheck():
    ClearAndUpdateQuery = '''UPDATE requested_date_audit_table
    SET requested_date_audit_table.[RequestedDate] = oe_hdr.requested_date
    FROM requested_date_audit_table
    INNER JOIN oe_hdr ON (oe_hdr.order_no = requested_date_audit_table.[Order No.]); 


    DELETE FROM requested_date_audit_table WHERE [Notified] = 'Y' OR [RequestedDate] > [Order Date] ;
    '''
    engine.execute(ClearAndUpdateQuery)


#Half of this formatting probably isn't needed.
def readTable():
    SelectQ = '''SELECT * from requested_date_audit_table;'''
    requesteddateaudittable = pd.read_sql_query(SelectQ, engine)
    RQDAuditTable = pd.DataFrame(requesteddateaudittable)
    RQDAuditTable = RQDAuditTable.loc[RQDAuditTable['Notified'] == 'N']
    RQDAuditTable['Order No.'] = RQDAuditTable['Order No.'].astype(int)
    RQDAuditTable['Customer ID'] = RQDAuditTable['Customer ID'].astype(int)
    RQDAuditTable['Sales Location'] = RQDAuditTable['Sales Location'].astype(int)
    RQDAuditTable['Order Date'] = pd.to_datetime(RQDAuditTable['Order Date'], format='%m-%d-%Y')
    RQDAuditTable['Order Date'] = RQDAuditTable['Order Date'].dt.strftime('%m-%d-%Y')
    RQDAuditTable['RequestedDate'] = pd.to_datetime(RQDAuditTable['RequestedDate'], format='%m-%d-%Y')
    RQDAuditTable['RequestedDate'] = RQDAuditTable['RequestedDate'].dt.strftime('%m-%d-%Y')
    RQDAuditTable = RQDAuditTable.loc[RQDAuditTable['RequestedDate'] <= RQDAuditTable['Order Date']]
    return RQDAuditTable


def UpdateSQL(OrderNo):
    UpdateQuery = f'''UPDATE requested_date_audit_table
    SET [Notified] = 'Y'
    FROM requested_date_audit_table
    WHERE [Order No.] = {OrderNo} OR
    [Sales Location]  = 120439;
    '''
    engine.execute(UpdateQuery)


sender = 'sqlagent@email.com'
receiver = 'me@e-mail.com'

SMTPserver  = 'SMTP.Server.Address'



while 1:
    DoubleCheck()
    RQDAuditTable = readTable()
    if len(RQDAuditTable) > 0:
        for index, column in RQDAuditTable.iterrows():
            if (RQDAuditTable.loc[index, 'Sales Location'] in [120439, 107390, 113401]) | (RQDAuditTable.loc[index, 'Order Taker'] in ['ADMIN', 'EDI_ORDER']):
                UpdateSQL(RQDAuditTable.loc[index, 'Order No.'])
            else:
                DoubleCheckDateQuery = ''''''
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
                            <p align="center" style="Margin: 0; font-size: 18px; Margin-bottom: 5px;">Requested Date <= Order Date</p></br>
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
                                                {RQDAuditTable.loc[index, 'Order No.']}
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
                                                {RQDAuditTable.loc[index, 'Order Taker']}
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
                                                {RQDAuditTable.loc[index, 'Customer ID']}
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
                                                {RQDAuditTable.loc[index, 'Customer Name']}
                                            </span></td>
                                    </tr>
                                    <tr>
                                        <td bgcolor="#FFFFFF" class="inner2 contents1"
                                            style="border:ridge;border-collapse:collapse;text-align:Right;padding-right:5px;padding-left:5px;padding-top:5px;padding-bottom:0px; font-family: verdana;font-size: 12px;">
                                            <span style="font-size:12px;">
                                                Sales Location:
                                            </span></td>
                                        <td bgcolor="#FFFFFF" class="inner2 contents1"
                                            style="border:ridge;border-collapse:collapse;text-align:Left;padding-right:5px;padding-left:5px;padding-top:5px;padding-bottom:0px;font-family: verdana;">
                                            <span style="font-size:12px;">
                                                {RQDAuditTable.loc[index, 'Sales Location']}
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
                                                {RQDAuditTable.loc[index, 'Order Date']}
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
                                                {RQDAuditTable.loc[index, 'RequestedDate']}
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
                msg['Subject'] = f"Same Day Order For {RQDAuditTable.loc[index, 'Order No.']}"
                msg['From'] = sender
                msg['To'] = receiver

                try:
                    with smtplib.SMTP(SMTPserver) as smtpObj:
                        smtpObj.send_message(msg)
                    UpdateSQL(RQDAuditTable.loc[index, 'Order No.'])
                except:
                    print("Error: unable to send email")
                    ctypes.windll.user32.MessageBoxW(0, f"{RQDAuditTable.loc[index,'Order Taker']} was not notified about Order No. {RQDAuditTable.loc[index, 'Order No.']}", "Failed to Send E-mail", 0x40000)
    else:
        pass

    time.sleep(900)
