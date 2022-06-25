import os
OMS_INFO_WAREHOUSE_LIST_END_POINT = os.environ.get('OMS_INFO_WAREHOUSE_LIST_END_POINT', '/open-api/oms/info/warehouse/list')
OMS_ORDER_BATCHSUBMIT_ENDPOINT=os.getenv("OMS_ORDER_BATCHSUBMIT_ENDPOINT", "/open-api/oms/order/batchSubmit")
OMS_ORDER_CANCEL_ENDPOINT=os.getenv("OMS_ORDER_CANCEL_ENDPOINT", "/open-api/oms/order/cancel")
OMS_ORDER_QUERY_ENDPOINT=os.getenv("OMS_ORDER_QUERY_ENDPOINT", "/open-api/oms/order/query")
OMS_ORDER_QUERYLISTV2_ENDPOINT=os.getenv("OMS_ORDER_QUERYLISTV2_ENDPOINT", "/open-api/oms/order/queryListV2")
OMS_PRODUCT_BATCHCREATE_ENDPOINT=os.getenv("OMS_PRODUCT_BATCHCREATE_ENDPOINT", "/open-api/oms/product/batchCreate")
OMS_PRODUCT_MODIFY_ENDPOINT=os.getenv("OMS_PRODUCT_MODIFY_ENDPOINT", "/open-api/oms/product/modify")
OMS_PRODUCT_QUERYLIST_ENDPOINT=os.getenv("OMS_PRODUCT_QUERYLIST_ENDPOINT", "/open-api/oms/product/queryList")
OMS_INBOUND_CANCEL_ENDPOINT=os.getenv("OMS_INBOUND_CANCEL_ENDPOINT", "/open-api/oms/inbound/cancel")
OMS_INBOUND_LIST_ENDPOINT=os.getenv("OMS_INBOUND_LIST_ENDPOINT", "/open-api/oms/inbound/list")
OMS_INBOUND_SAVEOMSINBOUNDORDER_ENDPOINT=os.getenv("OMS_INBOUND_SAVEOMSINBOUNDORDER_ENDPOINT", "/open-api/oms/inbound/saveOmsInboundOrder")
OMS_STOCK_LIST_ENDPOINT=os.getenv("OMS_STOCK_LIST_ENDPOINT", "/open-api/oms/stock/list")


