import marshmallow as ma
ProductAliasSchema = ma.Schema.from_dict(
                {
                    "omsSKu": ma.fields.String(description="系统SKU"),
                    "sku": ma.fields.String(description="店铺SKU"),
                    "storeId": ma.fields.String(description="店铺ID"),
                },name="product_alias",
            )
ProductBundleSchema = ma.Schema.from_dict(
    {
        "childQuantity": ma.fields.Integer(description="子SKU在组合中的数量"),
        "childSkuId": ma.fields.String(description="子SKU"),
        "note": ma.fields.String(description="组装信息"),
        "parentSkuId": ma.fields.String(description="系统SKU"),
    },name="bundle_detail",
)
ProductSchema = ma.Schema.from_dict(
    {
        "aliases": ma.fields.Nested(ProductAliasSchema, many=True, description="店铺别名详情"),
        "asin": ma.fields.String(description="ASIN码"),
        "auditRemark": ma.fields.String(description="审核备注"),
        "batteryFlag": ma.fields.String(description="是否有电池"),
        "batteryType": ma.fields.String(description="电池类型,含电池时必传. 1.Lithium Ion-Cells or batteries ONLY;2.Lithium Ion-Packed with Equipment;3.Lithium Ion-Contained in Equipment4.Lithium Metal-Cells or batteries ONLY5.Lithium Metal-Packed with Equipment6.Lithium Metal-Contained in Equipment"),
        "brand": ma.fields.String(description="品牌"),
        "bundleDetail": ma.fields.Nested(ProductBundleSchema, many=True),
        "custDescription": ma.fields.String(description="产品海关描述"),
        "declaredValue": ma.fields.Float(description="货值"),
        "distanceUnit": ma.fields.String(description="长度单位，仅支持 in，cm"),
        "ean": ma.fields.String(description="EAN码"),
        "extraBarcode1": ma.fields.String(description="额外编码"),
        "fnSku": ma.fields.String(description="FNSKU码"),
        "height": ma.fields.Float(description="高度"),
        "individualInventory": ma.fields.Integer(description="1.仅有独立库存 2.独立库存+非独立库存"),
        "length": ma.fields.Float(description="长度"),
        "massUnit": ma.fields.String(description="重量单位，仅支持“lb”，“kg”"),
        "note": ma.fields.String(description="备注"),
        "omsSku": ma.fields.String(description="系统SKU"),
        "orgId": ma.fields.String(description="客户机构号"),
        "originCountry": ma.fields.String(description="原产地国家，格式标准遵循ISO 3166-1 alpha-2"),
        "purchasingCost": ma.fields.Float(description="采购金额"),
        "qtyInOnePackage": ma.fields.Integer(description="单包裹产品数量,运输类型为直接运输有效"),
        "quantityUnit": ma.fields.String(description="数量单位（来自于承运商要求）"),
        "scheduleB": ma.fields.String(description="海关协调码"),
        "shippingType": ma.fields.String(description="运输类型，1.直接运输 2.需要仓库另外打包"),
        "skuId": ma.fields.String(),
        "skuNameCN": ma.fields.String(description="产品描述"),
        "skuNameEN": ma.fields.String(description="产品名称"),
        "status": ma.fields.Integer(description="状态，1.待审核 2.已审核 3.已归档"),
        "type": ma.fields.Integer(description="类型 1.单个产品 2.组合产品"),
        "upc": ma.fields.String(description="UPC码"),
        "valueCurrency": ma.fields.String(description="货值币种，格式标准遵循ISO 4217"),
        "warehouseIds": ma.fields.List(ma.fields.String(description="仓库ID"), description="新增/修改 产品时选择的仓库列表"),
        "weight1": ma.fields.Float(description="主重量，根据重量单位的不同，展示kg或者是lb"),
        "weight2": ma.fields.Float(description="次重量，根据重量单位的不同，展示g或者是oz，与主重量相加即为总重量"),
        "width": ma.fields.Float(description="宽度"),
    }, name="product",
)

StockInventorySchema = ma.Schema.from_dict(
    {
        "brokenQuantity": ma.fields.Integer(description="破损库存数"),
        "inboundingQuantity": ma.fields.Integer(description="入库库存数"),
        "omsSku": ma.fields.String(description="SKU"),
        "outboundingQuantity": ma.fields.Integer(description="出库占用库存数"),
        "pickedQuantity": ma.fields.Integer(description="拣货缓存库存数"),
        "skuNameEN": ma.fields.String(description="产品名称"),
        "skuType": ma.fields.Integer(description="产品类型, 1：单个产品 2：组合产品"),
        "standardQuantity": ma.fields.Integer(description="标品库存数"),
        "warehouseId": ma.fields.String(description="仓库编号"),
    },
    name="stock",
)

WarehouseSchema = ma.Schema.from_dict(
    {
        "orgId": ma.fields.String(description="仓库机构ID"),
        "timeZone": ma.fields.String(description="仓库时区"),
        "warehouseAddr1": ma.fields.String(description="仓库地址1"),
        "warehouseAddr2": ma.fields.String(description="仓库地址2"),
        "warehouseCity": ma.fields.String(description="仓库所在城市"),
        "warehouseContacts": ma.fields.String(description="仓库联系人"),
        "warehouseCountry": ma.fields.String(description="仓库所在国家"),
        "warehouseEmail": ma.fields.String(description="仓库联系Email"),
        "warehouseId": ma.fields.String(description="仓库编号"),
        "warehouseName": ma.fields.String(description="仓库名称"),
        "warehousePhone": ma.fields.String(description="仓库联系电话"),
        "warehouseProvince": ma.fields.String(description="仓库所在州"),
        "warehouseZipCode": ma.fields.String(description="仓库邮编"),
    },
    name="warehouse",
)


WMSProductSchema = ma.Schema.from_dict(
    {

    }
)


from marshmallow_jsonschema import JSONSchema
import json

with open("schemas/warehouses.json",'w',encoding='utf-8') as f:
    json_schema = JSONSchema()
    json.dump(json_schema.dump(WarehouseSchema()),f,ensure_ascii=False,indent=4)
with open("schemas/products.json",'w',encoding='utf-8') as f:
    json_schema = JSONSchema()
    json.dump(json_schema.dump(ProductSchema()),f,ensure_ascii=False,indent=4)
with open("schemas/stocks.json",'w',encoding='utf-8') as f:
    json_schema = JSONSchema()
    json.dump(json_schema.dump(StockInventorySchema()),f,ensure_ascii=False,indent=4)




