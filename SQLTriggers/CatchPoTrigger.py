
###SQL Table and Trigger creation, python pulse check and notification follows.




# USE [CommerceCenter]
# GO
# CREATE TRIGGER 
#  on [po_line]
# FOR INSERT 
# AS DECLARE @Creator VARCHAR(25),
# 	   @PO_No decimal(19,0),
# 	   @Item VARCHAR(40),
# 	   @Item_Description VARCHAR(40),
# 	   @SupplierID decimal(19,0),
# 	   @SupplierName VARCHAR(40),
# 	   @PurchasePrice decimal(19,5),
# 	   @Order_Qty decimal(9,0),
# 	   @Order_Total decimal(19,5),
# 	   @PurchaseUOM CHAR(8),
# 	   @DistNetPrice decimal(19,5),
# 		@UOM_QTY decimal(9,0),
# 		@TotalOffDistNet decimal(19,5),
# 	   @Approved CHAR(1),
# 	   @Cancel_flag CHAR(1),
# 	   @Delete_flag CHAR(1)
# 	   ;

# SELECT @Creator = ins.created_by FROM INSERTED ins;
# SELECT @PO_No = ins.po_no FROM INSERTED ins;
# SELECT @Item = inv_mast.item_id FROM INSERTED ins INNER JOIN inv_mast ON (inv_mast.inv_mast_uid = ins.inv_mast_uid);
# SELECT @Item_Description = ins.item_description FROM INSERTED ins;
# SELECT @SupplierID = po_hdr.supplier_id FROM INSERTED ins INNER JOIN po_hdr ON (ins.po_no = po_hdr.po_no);
# SELECT @SupplierName = supplier.supplier_name FROM supplier WHERE supplier.supplier_id = @SupplierID;
# SELECT @PurchasePrice = ins.unit_price FROM INSERTED ins;
# SELECT @PurchaseUOM = ins.pricing_unit FROM INSERTED ins;
# SELECT @Order_Qty = ins.qty_ordered FROM INSERTED ins;
# SELECT @Order_Total = ROUND(@PurchasePrice*@Order_Qty,2);
# SELECT @UOM_QTY = item_uom.unit_size FROM item_uom INNER JOIN INSERTED ins ON (ins.inv_mast_uid = item_uom.inv_mast_uid) WHERE item_uom.unit_of_measure = ins.pricing_unit;
# SELECT @DistNetPrice = inv_sup.cost FROM INSERTED ins INNER JOIN inventory_supplier inv_sup ON (ins.inv_mast_uid = inv_sup.inv_mast_uid) WHERE inv_sup.primary_supplier_flag = 'Y';
# SELECT @Approved = po_hdr.approved FROM INSERTED ins INNER JOIN po_hdr ON (ins.po_no = po_hdr.po_no);
# SELECT @Cancel_flag = ins.cancel_flag FROM INSERTED ins;
# SELECT @Delete_flag = ins.delete_flag FROM INSERTED ins;
# SELECT @TotalOffDistNet = ROUND(@DistNetPrice*@UOM_QTY*@Order_Qty,2);

# IF @Approved = 'Y' 
# AND @SupplierID IN (106715,104807,104624,105220,108284,105950,104311,104289,104242,106724,105268,105248,107206,105093,104318 ) 

# AND @Order_Total <> @TotalOffDistNet
# AND @Cancel_flag <> 'Y'
# AND @Delete_flag <> 'Y'
# 	BEGIN

# 		INSERT INTO [po_pricing_audit_table]( 
# 		[Creator]
# 		,[PO No.]
# 		,[Item ID]
# 		,[Item Description]
# 		,[Supplier ID]
# 		,[Supplier Name]
# 		,[Purchase Price]
# 		,[PurchaseUOM]
# 		,[Order_Qty]
# 		,[Order Total]
# 		,[Dist Net Price]
# 		,[UOM_QTY]
# 		,[TotalOffDistNet]
# 		,[Notified])
# 		VALUES (@Creator,
# 		@PO_No,
# 		@Item,
# 		@Item_Description,
# 		@SupplierID,
# 		@SupplierName,
# 		@PurchasePrice,
# 		@PurchaseUOM,
# 		@Order_Qty,
# 		@Order_Total,
# 		@DistNetPrice,
# 		@UOM_QTY,
# 		@TotalOffDistNet,
# 		'N');
# 	END
# ELSE
# 		BEGIN
# 			return
# 		END


# USE [DATABASE]
# GO

# SET ANSI_NULLS ON
# GO

# SET QUOTED_IDENTIFIER ON
# GO

# SET ANSI_PADDING ON
# GO

# CREATE TABLE [dbo].[po_pricing_audit_table](
# 		[Creator]  VARCHAR(25),
# 		[PO No.]  decimal(19,0),
# 		[Item ID] VARCHAR(40),
# 		[Item Description] VARCHAR(40),
# 		[Supplier ID] decimal(19,0),
# 		[Supplier Name] VARCHAR(40),
# 		[Purchase Price] decimal(19,5),
# 		[PurchaseUOM] CHAR(8),
# 		[Order_Qty] decimal(9,0),
# 		[Order Total] decimal(19,5),
# 		[Dist Net Price] decimal(19,5),
# 		[UOM_QTY] CHAR(8),
# 		[TotalOffDistNet] decimal(19,5),
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


def readTable():
    SelectQ = '''SELECT * from po_pricing_audit_table ;'''
    poaudittabledata = pd.read_sql_query(SelectQ, engine)
    POAuditTable = pd.DataFrame(poaudittabledata)
    POAuditTable = POAuditTable.loc[POAuditTable['Notified'] == 'N']
    POAuditTable['PO No.'] = POAuditTable['PO No.'].astype(int)
    POAuditTable['Supplier ID'] = POAuditTable['Supplier ID'].astype(int)
    POAuditTable['Creator'] = POAuditTable['Creator'].str.replace('diversified\\\\', '')
    POAuditTable['Creator'] = POAuditTable['Creator'].str.replace('DIVERSIFIED\\\\', '')
    return POAuditTable


def UpdateSQL(PoNo, ItemID):
    UpdateQuery = f'''UPDATE po_pricing_audit_table
    SET [Notified] = 'Y'
    FROM po_pricing_audit_table
    WHERE [PO No.] = {PoNo} AND
    [Item ID] = '{ItemID}' ;
    '''

    engine.execute(UpdateQuery)


sender = 'our sql server assistant'
receiver = 'me'

SMTPserver  = 'SMTP.server.address'


while 1:
    PoAuditTable = readTable()
    if len(PoAuditTable) > 0:
        for index, column in PoAuditTable.iterrows():


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
                        <p align="center" style="Margin: 0; font-size: 18px; Margin-bottom: 5px;">PO Pricing Discrepancy</p></br>
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
                                            PO #: 
                                        </span></td>
                                    <td bgcolor="#FFFFFF" class="inner2 contents1"
                                        style="border:ridge;border-collapse:collapse;text-align:Left;padding-right:5px;padding-left:5px;padding-top:5px;padding-bottom:0px;font-family: verdana;">
                                        <span style="font-size:12px;">
                                            {PoAuditTable.loc[index, 'PO No.']}
                                        </span></td>
                                </tr>

                                <tr>
                                    <td bgcolor="#FFFFFF" class="inner2 contents1"
                                        style="border:ridge;border-collapse:collapse;text-align:Right;padding-right:5px;padding-left:5px;padding-top:5px;padding-bottom:0px; font-family: verdana;font-size: 12px;">
                                        <span style="font-size:12px;">
                                            Creator:
                                        </span></td>
                                    <td bgcolor="#FFFFFF" class="inner2 contents1"
                                        style="border:ridge;border-collapse:collapse;text-align:Left;padding-right:5px;padding-left:5px;padding-top:5px;padding-bottom:0px;font-family: verdana;">
                                        <span style="font-size:12px;">
                                            {PoAuditTable.loc[index, 'Creator']}
                                        </span></td>
                                </tr>
                                <tr>
                                    <td bgcolor="#FFFFFF" class="inner2 contents1"
                                        style="border:ridge;border-collapse:collapse;text-align:Right;padding-right:5px;padding-left:5px;padding-top:5px;padding-bottom:0px; font-family: verdana;font-size: 12px;">
                                        <span style="font-size:12px;">
                                            Supplier ID:
                                        </span></td>
                                    <td bgcolor="#FFFFFF" class="inner2 contents1"
                                        style="border:ridge;border-collapse:collapse;text-align:Left;padding-right:5px;padding-left:5px;padding-top:5px;padding-bottom:0px;font-family: verdana;">
                                        <span style="font-size:12px;">
                                            {PoAuditTable.loc[index, 'Supplier ID']}
                                        </span></td>
                                </tr>

                                <tr>
                                    <td bgcolor="#FFFFFF" class="inner2 contents1"
                                        style="border:ridge;border-collapse:collapse;text-align:Right;padding-right:5px;padding-left:5px;padding-top:5px;padding-bottom:0px; font-family: verdana;font-size: 12px;">
                                        <span style="font-size:12px;">
                                            Supplier Name:
                                        </span></td>
                                    <td bgcolor="#FFFFFF" class="inner2 contents1"
                                        style="border:ridge;border-collapse:collapse;text-align:Left;padding-right:5px;padding-left:5px;padding-top:5px;padding-bottom:0px;font-family: verdana;">
                                        <span style="font-size:12px;">
                                            {PoAuditTable.loc[index, 'Supplier Name']}
                                        </span></td>
                                </tr>
                                <tr>
                                    <td bgcolor="#FFFFFF" class="inner2 contents1"
                                        style="border:ridge;border-collapse:collapse;text-align:Right;padding-right:5px;padding-left:5px;padding-top:5px;padding-bottom:0px; font-family: verdana;font-size: 12px;">
                                        <span style="font-size:12px;">
                                            Item INFO:
                                        </span></td>
                                    <td bgcolor="#FFFFFF" class="inner2 contents1"
                                        style="border:ridge;border-collapse:collapse;text-align:Left;padding-right:5px;padding-left:5px;padding-top:5px;padding-bottom:0px;font-family: verdana;">
                                        <span style="font-size:12px;">
                                            {PoAuditTable.loc[index, 'Item ID']}<br> {PoAuditTable.loc[index, 'Item Description'] }
                                        </span></td>
                                </tr>
                                <tr>
                                    <td bgcolor="#FFFFFF" class="inner2 contents1"
                                        style="border:ridge;border-collapse:collapse;text-align:Right;padding-right:5px;padding-left:5px;padding-top:5px;padding-bottom:0px; font-family: verdana;font-size: 12px;">
                                        <span style="font-size:12px;">
                                            Purchase Price:
                                        </span></td>
                                    <td bgcolor="#FFFFFF" class="inner2 contents1"
                                        style="border:ridge;border-collapse:collapse;text-align:Left;padding-right:5px;padding-left:5px;padding-top:5px;padding-bottom:0px;font-family: verdana;">
                                        <span style="font-size:12px;">
                                            {PoAuditTable.loc[index, 'Purchase Price']}
                                        </span></td>
                                </tr>
                                <tr>
                                    <td bgcolor="#FFFFFF" class="inner2 contents1"
                                        style="border:ridge;border-collapse:collapse;text-align:Right;padding-right:5px;padding-left:5px;padding-top:5px;padding-bottom:0px; font-family: verdana;font-size: 12px;">
                                        <span style="font-size:12px;">
                                            Purchase Price UOM:
                                        </span></td>
                                    <td bgcolor="#FFFFFF" class="inner2 contents1"
                                        style="border:ridge;border-collapse:collapse;text-align:Left;padding-right:5px;padding-left:5px;padding-top:5px;padding-bottom:0px;font-family: verdana;">
                                        <span style="font-size:12px;">
                                            {PoAuditTable.loc[index, 'PurchaseUOM']}
                                        </span></td>
                                </tr>
                                                                <tr>
                                    <td bgcolor="#FFFFFF" class="inner2 contents1"
                                        style="border:ridge;border-collapse:collapse;text-align:Right;padding-right:5px;padding-left:5px;padding-top:5px;padding-bottom:0px; font-family: verdana;font-size: 12px;">
                                        <span style="font-size:12px;">
                                            Order Qty:
                                        </span></td>
                                    <td bgcolor="#FFFFFF" class="inner2 contents1"
                                        style="border:ridge;border-collapse:collapse;text-align:Left;padding-right:5px;padding-left:5px;padding-top:5px;padding-bottom:0px;font-family: verdana;">
                                        <span style="font-size:12px;">
                                            {PoAuditTable.loc[index, 'Order_Qty']}
                                        </span></td>
                                </tr>
                                                                </tr>
                                                                <tr>
                                    <td bgcolor="#FFFFFF" class="inner2 contents1"
                                        style="border:ridge;border-collapse:collapse;text-align:Right;padding-right:5px;padding-left:5px;padding-top:5px;padding-bottom:0px; font-family: verdana;font-size: 12px;">
                                        <span style="font-size:12px;">
                                            Order Total:
                                        </span></td>
                                    <td bgcolor="#FFFFFF" class="inner2 contents1"
                                        style="border:ridge;border-collapse:collapse;text-align:Left;padding-right:5px;padding-left:5px;padding-top:5px;padding-bottom:0px;font-family: verdana;">
                                        <span style="font-size:12px;">
                                            {PoAuditTable.loc[index, 'Order Total']}
                                        </span></td>
                                </tr>
                                <tr>
                                    <td bgcolor="#FFFFFF" class="inner2 contents1"
                                        style="border:ridge;border-collapse:collapse;text-align:Right;padding-right:5px;padding-left:5px;padding-top:5px;padding-bottom:0px; font-family: verdana;font-size: 12px;">
                                        <span style="font-size:12px;">
                                            Assumed Dist. Net:
                                        </span></td>
                                    <td bgcolor="#FFFFFF" class="inner2 contents1"
                                        style="border:ridge;border-collapse:collapse;text-align:Left;padding-right:5px;padding-left:5px;padding-top:5px;padding-bottom:0px;font-family: verdana;">
                                        <span style="font-size:12px;">
                                            {PoAuditTable.loc[index, 'Dist Net Price']}
                                        </span></td>
                                </tr>
                                <tr>
                                    <td bgcolor="#FFFFFF" class="inner2 contents1"
                                        style="border:ridge;border-collapse:collapse;text-align:Right;padding-right:5px;padding-left:5px;padding-top:5px;padding-bottom:0px; font-family: verdana;font-size: 12px;">
                                        <span style="font-size:12px;">
                                            UOM QTY:
                                        </span></td>
                                    <td bgcolor="#FFFFFF" class="inner2 contents1"
                                        style="border:ridge;border-collapse:collapse;text-align:Left;padding-right:5px;padding-left:5px;padding-top:5px;padding-bottom:0px;font-family: verdana;">
                                        <span style="font-size:12px;">
                                            {PoAuditTable.loc[index, 'UOM_QTY']}
                                        </span></td>
                                </tr>
                                <tr>
                                    <td bgcolor="#FFFFFF" class="inner2 contents1"
                                        style="border:ridge;border-collapse:collapse;text-align:Right;padding-right:5px;padding-left:5px;padding-top:5px;padding-bottom:0px; font-family: verdana;font-size: 12px;">
                                        <span style="font-size:12px;">
                                            Total Off of Dist. Net * UOM QTY:
                                        </span></td>
                                    <td bgcolor="#FFFFFF" class="inner2 contents1"
                                        style="border:ridge;border-collapse:collapse;text-align:Left;padding-right:5px;padding-left:5px;padding-top:5px;padding-bottom:0px;font-family: verdana;">
                                        <span style="font-size:12px;">
                                            {PoAuditTable.loc[index, 'TotalOffDistNet']}
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
            msg['Subject'] = f"Pricing Discrepancy For PO# {PoAuditTable.loc[index, 'PO No.']}"
            msg['From'] = sender
            msg['To'] = receiver
            try:
                with smtplib.SMTP(SMTPserver) as smtpObj:
                    smtpObj.send_message(msg)
                UpdateSQL(PoAuditTable.loc[index, 'PO No.'], PoAuditTable.loc[index, 'Item ID'])
            except:
                print("Error: unable to send email")
                ctypes.windll.user32.MessageBoxW(0, f"{PoAuditTable.loc[index,'Creator']} was not notified about PO No. {PoAuditTable.loc[index, 'PO No.']}", "Failed to Send E-mail", 0x40000)
    else:
        pass

    time.sleep(900)





