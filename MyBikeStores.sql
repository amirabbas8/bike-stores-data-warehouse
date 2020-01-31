create database MyBikeStores
go
create schema production
go

create schema sales
go

create table production.brands
(
	brand_id int identity
		primary key,
	brand_name varchar(255) not null
)
go

create table production.categories
(
	category_id int identity
		primary key,
	category_name varchar(255) not null
)
go

create table sales.contract_contractors
(
	id int identity
		primary key,
	name varchar(200),
	phone varchar(200)
)
go

create table sales.customer_service_type
(
	id int identity
		primary key,
	type varchar(50),
	description varchar(500)
)
go

create table sales.customers
(
	customer_id int identity
		primary key,
	first_name varchar(255) not null,
	last_name varchar(255) not null,
	phone varchar(25),
	email varchar(255) not null,
	street varchar(255),
	city varchar(50),
	state varchar(25),
	zip_code varchar(5)
)
go

create table sales.customer_services
(
	id int identity,
	customer_id int
		constraint FK_customer_services_id
			references sales.customers,
	type int
		references sales.customer_service_type,
	date date,
	cost numeric(18),
	description varchar(500)
)
go

create table sales.gift_cards
(
	id int identity
		primary key,
	gift_code varchar(50),
	amount numeric(18),
	count int
)
go

create table sales.gift_card_usages
(
	id int identity
		primary key,
	card int
		references sales.gift_cards,
	customer int
		references sales.customers,
	date date
)
go

create table production.products
(
	product_id int identity
		primary key,
	product_name varchar(255) not null,
	brand_id int not null
		references production.brands
			on update cascade on delete cascade,
	category_id int not null
		references production.categories
			on update cascade on delete cascade,
	model_year smallint not null,
	list_price decimal(10,2) not null
)
go

create table sales.stores
(
	store_id int identity
		primary key,
	store_name varchar(255) not null,
	phone varchar(25),
	email varchar(255),
	street varchar(255),
	city varchar(255),
	state varchar(10),
	zip_code varchar(5)
)
go

create table sales.contracts
(
	id int identity
		primary key,
	store int
		references sales.stores,
	contractor int
		references sales.contract_contractors,
	subject varchar(200),
	description varchar(500),
	document_code varchar(200)
)
go

create table sales.marketing_campaigns
(
	id int identity
		primary key,
	name varchar(100),
	description varchar(500),
	budget numeric(18),
	chanel varchar(50),
	store_id int
		constraint fk_mc_store
			references sales.stores
)
go

create table sales.staffs
(
	staff_id int identity
		primary key,
	first_name varchar(50) not null,
	last_name varchar(50) not null,
	email varchar(255) not null
		unique,
	phone varchar(25),
	active tinyint not null,
	store_id int not null
		references sales.stores
			on update cascade on delete cascade,
	manager_id int
		references sales.staffs
)
go

create table sales.orders
(
	order_id int identity
		primary key,
	customer_id int
		references sales.customers
			on update cascade on delete cascade,
	order_date date not null,
	required_date date not null,
	store_id int not null
		references sales.stores
			on update cascade on delete cascade,
	staff_id int not null
		references sales.staffs
)
go

create table sales.guaranty
(
	order_id int not null
		primary key
		constraint FK_guaranty_id
			references sales.orders,
	start_date date,
	finish_date date
)
go

create table sales.order_items
(
	order_id int not null
		references sales.orders
			on update cascade on delete cascade,
	item_id int not null,
	product_id int not null
		references production.products
			on update cascade on delete cascade,
	quantity int not null,
	list_price decimal(10,2) not null,
	discount decimal(4,2) default 0 not null,
	primary key (order_id, item_id)
)
go

create table production.stocks
(
	store_id int not null
		references sales.stores
			on update cascade on delete cascade,
	product_id int not null
		references production.products
			on update cascade on delete cascade,
	quantity int,
	primary key (store_id, product_id)
)
go

