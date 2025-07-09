 -- Pitman Arm

create table if not EXISTS crawl_ebay_pitmanarm_listing1(
	vc_item VARCHAR(255) comment '标题',
	vc_item_link TEXT comment '标题链接',
	d_sell_price VARCHAR(255) comment '售价-美元',
	d_num1 VARCHAR(255) comment '销售数量',
	d_num2 VARCHAR(255) comment '销售数量',
	vc_type VARCHAR(255) comment '类型',
	vc_brand VARCHAR(255) comment '品牌',
	vc_manufacturer_part_number VARCHAR(255) comment '制造商零部件编号',
	vc_oe_number VARCHAR(255) comment 'OE/OEM number moog号标准',
	vc_items_included VARCHAR(255) comment '包含物品',
	vc_placement_on_vehicle VARCHAR(255) comment '在车轮的位置',
	vc_interchange_part_num TEXT comment '互换零件编号'
) COMMENT '球头1';

create table if not EXISTS crawl_ebay_pitmanarm_listing2(
	vc_item VARCHAR(255) comment '标题',
	vc_item_link TEXT comment '标题链接',
	d_sell_price VARCHAR(255) comment '售价-美元',
	d_num1 VARCHAR(255) comment '销售数量',
	d_num2 VARCHAR(255) comment '销售数量',
	vc_type VARCHAR(255) comment '类型',
	vc_brand VARCHAR(255) comment '品牌',
	vc_manufacturer_part_number VARCHAR(255) comment '制造商零部件编号',
	vc_oe_number VARCHAR(255) comment 'OE/OEM number moog号标准',
	vc_items_included VARCHAR(255) comment '包含物品',
	vc_placement_on_vehicle VARCHAR(255) comment '在车轮的位置',
	vc_interchange_part_num TEXT comment '互换零件编号'
) COMMENT '球头2';


create table if not EXISTS crawl_ebay_balljoint_listing1(
	vc_item VARCHAR(255) comment '标题',
	vc_item_link TEXT comment '标题链接',
	d_sell_price VARCHAR(255) comment '售价-美元',
	d_num1 VARCHAR(255) comment '销售数量',
	d_num2 VARCHAR(255) comment '销售数量',
	vc_type VARCHAR(255) comment '类型',
	vc_brand VARCHAR(255) comment '品牌',
	vc_manufacturer_part_number VARCHAR(255) comment '制造商零部件编号',
	vc_oe_number VARCHAR(255) comment 'OE/OEM number moog号标准',
	vc_items_included VARCHAR(255) comment '包含物品',
	vc_placement_on_vehicle VARCHAR(255) comment '在车轮的位置',
	vc_interchange_part_num TEXT comment '互换零件编号'
) COMMENT '球头1';

create table if not EXISTS crawl_ebay_balljoint_listing2(
	vc_item VARCHAR(255) comment '标题',
	vc_item_link TEXT comment '标题链接',
	d_sell_price VARCHAR(255) comment '售价-美元',
	d_num1 VARCHAR(255) comment '销售数量',
	d_num2 VARCHAR(255) comment '销售数量',
	vc_type VARCHAR(255) comment '类型',
	vc_brand VARCHAR(255) comment '品牌',
	vc_manufacturer_part_number VARCHAR(255) comment '制造商零部件编号',
	vc_oe_number VARCHAR(255) comment 'OE/OEM number moog号标准',
	vc_items_included VARCHAR(255) comment '包含物品',
	vc_placement_on_vehicle VARCHAR(255) comment '在车轮的位置',
	vc_interchange_part_num TEXT comment '互换零件编号'
) COMMENT '球头2';

 create table if not EXISTS crawl_ebay_idlerarm_listing1(
	vc_item VARCHAR(255) comment '标题',
	vc_item_link TEXT comment '标题链接',
	d_sell_price VARCHAR(255) comment '售价-美元',
	d_num1 VARCHAR(255) comment '销售数量',
	d_num2 VARCHAR(255) comment '销售数量',
	vc_type VARCHAR(255) comment '类型',
	vc_brand VARCHAR(255) comment '品牌',
	vc_manufacturer_part_number VARCHAR(255) comment '制造商零部件编号',
	vc_oe_number VARCHAR(255) comment 'OE/OEM number moog号标准',
	vc_items_included VARCHAR(255) comment '包含物品',
	vc_placement_on_vehicle VARCHAR(255) comment '在车轮的位置',
	vc_interchange_part_num TEXT comment '互换零件编号'
) COMMENT '惰轮臂1';

create table if not EXISTS crawl_ebay_idlerarm_listing2(
	vc_item VARCHAR(255) comment '标题',
	vc_item_link TEXT comment '标题链接',
	d_sell_price VARCHAR(255) comment '售价-美元',
	d_num1 VARCHAR(255) comment '销售数量',
	d_num2 VARCHAR(255) comment '销售数量',
	vc_type VARCHAR(255) comment '类型',
	vc_brand VARCHAR(255) comment '品牌',
	vc_manufacturer_part_number VARCHAR(255) comment '制造商零部件编号',
	vc_oe_number VARCHAR(255) comment 'OE/OEM number moog号标准',
	vc_items_included VARCHAR(255) comment '包含物品',
	vc_placement_on_vehicle VARCHAR(255) comment '在车轮的位置',
	vc_interchange_part_num TEXT comment '互换零件编号'
) COMMENT '惰轮臂2';

 create table if not EXISTS crawl_ebay_tierodend_listing1(
	vc_item VARCHAR(255) comment '标题',
	vc_item_link TEXT comment '标题链接',
	d_sell_price VARCHAR(255) comment '售价-美元',
	d_num1 VARCHAR(255) comment '销售数量',
	d_num2 VARCHAR(255) comment '销售数量',
	vc_type VARCHAR(255) comment '类型',
	vc_brand VARCHAR(255) comment '品牌',
	vc_manufacturer_part_number VARCHAR(255) comment '制造商零部件编号',
	vc_oe_number VARCHAR(255) comment 'OE/OEM number moog号标准',
	vc_items_included VARCHAR(255) comment '包含物品',
	vc_placement_on_vehicle VARCHAR(255) comment '在车轮的位置',
	vc_interchange_part_num TEXT comment '互换零件编号'
) COMMENT '球头拉杆1';

create table if not EXISTS crawl_ebay_tierodend_listing2(
	vc_item VARCHAR(255) comment '标题',
	vc_item_link TEXT comment '标题链接',
	d_sell_price VARCHAR(255) comment '售价-美元',
	d_num1 VARCHAR(255) comment '销售数量',
	d_num2 VARCHAR(255) comment '销售数量',
	vc_type VARCHAR(255) comment '类型',
	vc_brand VARCHAR(255) comment '品牌',
	vc_manufacturer_part_number VARCHAR(255) comment '制造商零部件编号',
	vc_oe_number VARCHAR(255) comment 'OE/OEM number moog号标准',
	vc_items_included VARCHAR(255) comment '包含物品',
	vc_placement_on_vehicle VARCHAR(255) comment '在车轮的位置',
	vc_interchange_part_num TEXT comment '互换零件编号'
) COMMENT '球头拉杆2';

 create table if not EXISTS crawl_ebay_swaybarendlink_listing1(
	vc_item VARCHAR(255) comment '标题',
	vc_item_link TEXT comment '标题链接',
	d_sell_price VARCHAR(255) comment '售价-美元',
	d_num1 VARCHAR(255) comment '销售数量',
	d_num2 VARCHAR(255) comment '销售数量',
	vc_type VARCHAR(255) comment '类型',
	vc_brand VARCHAR(255) comment '品牌',
	vc_manufacturer_part_number VARCHAR(255) comment '制造商零部件编号',
	vc_oe_number VARCHAR(255) comment 'OE/OEM number moog号标准',
	vc_items_included VARCHAR(255) comment '包含物品',
	vc_placement_on_vehicle VARCHAR(255) comment '在车轮的位置',
	vc_interchange_part_num TEXT comment '互换零件编号'
) COMMENT '稳定杆连接杆1';

create table if not EXISTS crawl_ebay_swaybarendlink_listing2(
	vc_item VARCHAR(255) comment '标题',
	vc_item_link TEXT comment '标题链接',
	d_sell_price VARCHAR(255) comment '售价-美元',
	d_num1 VARCHAR(255) comment '销售数量',
	d_num2 VARCHAR(255) comment '销售数量',
	vc_type VARCHAR(255) comment '类型',
	vc_brand VARCHAR(255) comment '品牌',
	vc_manufacturer_part_number VARCHAR(255) comment '制造商零部件编号',
	vc_oe_number VARCHAR(255) comment 'OE/OEM number moog号标准',
	vc_items_included VARCHAR(255) comment '包含物品',
	vc_placement_on_vehicle VARCHAR(255) comment '在车轮的位置',
	vc_interchange_part_num TEXT comment '互换零件编号'
) COMMENT '稳定杆连接杆2';