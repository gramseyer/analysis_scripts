import requests

from stellar_sdk import xdr as stellar_xdr

def get_horizon_json(ledger_num):
	url = "https://horizon.stellar.org/ledgers/" + str(ledger_num) + "/transactions?limit=200&include_failed=true"
	#print(url)
	r = requests.get(url)
	return r.json()


def parse_tx(tx_envelope_json):
	return stellar_xdr.TransactionEnvelope.from_xdr(tx_envelope_json['envelope_xdr'])

def demux_account(muxed_account):
	if (muxed_account.type == stellar_xdr.CryptoKeyType.KEY_TYPE_ED25519):
		return muxed_account.ed25519.to_xdr()
	raise ValueError("what's a muxed account")


class TxObj:
	def __init__(self, sourceAccount, feeAccount, envelope):
		self.sourceAccount = sourceAccount
		self.feeAccount = feeAccount
		self.envelope = envelope

		if self.envelope.type == stellar_xdr.EnvelopeType.ENVELOPE_TYPE_TX_V0:
			self.operations = self.envelope.v0.tx.operations
		elif self.envelope.type == stellar_xdr.EnvelopeType.ENVELOPE_TYPE_TX:
			self.operations = self.envelope.v1.tx.operations
		else:
			self.feeAccount = demux_account(self.envelope.fee_bump.tx.fee_source)
			self.operations = self.envelope.fee_bump.tx.inner_tx.v1.tx.operations

from enum import Enum

class ConflictReason(Enum):
	MARKET = 1
	ACCOUNT_BALANCE = 2
	DATA = 3
	ACCOUNT = 4
	UNKNOWN = 5


class ConflictStats:
	def __init__(self, optype, reason, empty = False):
		if (empty):
			self.conflicts = {}
			self.reasons = {}
		else:
			self.conflicts = {}
			if optype is not None:
				self.conflicts[optype] = 1
			else:
				self.conflicts["Overall_Tx"] = 1
			self.reasons = {}
			self.reasons[reason] = 1

	def add(self, other):
		for conflict in other.conflicts.keys():
			if conflict not in self.conflicts.keys():
				self.conflicts[conflict] = 0
			self.conflicts[conflict] += other.conflicts[conflict]
		for reason in other.reasons.keys():
			if reason not in self.reasons.keys():
				self.reasons[reason] = 0
			self.reasons[reason] += other.reasons[reason]


def get_tx_list(json_obj):
	objs = []
	i = 0
	records = json_obj["_embedded"]["records"]
	while True:
		for record in records:
			src = record["source_account"]
			fee = record["fee_account"]
			envelope = parse_tx(record)
			objs.append(TxObj(src, fee, envelope))
		else:
			return objs

def get_transactions(ledger_num):
	res = get_horizon_json(ledger_num)
	return get_tx_list(res)


#Checks conflicts.
class ConflictModel:
	def __init__(self):
		self.touched_account_balances_up = set([])
		self.touched_account_balances_down = set([])
		self.touched_markets = set([])
		self.touched_accounts = set([])
		self.managed_data_map = {}

	def check_market(self, sell, buy):
		keyA = sell + "_" + buy
		keyB = buy + "_" + sell
		return keyA not in self.touched_markets and keyB not in self.touched_markets

	def commit_market(self, sell, buy):
		keyA = sell + "_" + buy
		keyB = buy + "_" + sell
		self.touched_markets.add(keyA)
		self.touched_markets.add(keyB)

	def account_balance_key(self, account, currency):
		return account + "_" + currency


	#it turns out that up and down are symmetrically conflicting because of trustline/overflow limits.
	def check_account_balance_up(self, account, currency):
		key = self.account_balance_key(account, currency)
		return not (key in self.touched_account_balances_up or key in self.touched_account_balances_down)

	def check_account_balance_down(self, account, currency):
		key = self.account_balance_key(account, currency)
		return not (key in self.touched_account_balances_down or key in self.touched_account_balances_up)

	def commit_account_balance_up(self, account, currency):
		key = self.account_balance_key(account, currency)
		self.touched_account_balances_up.add(key)

	def commit_account_balance_down(self, account, currency):
		key = self.account_balance_key(account, currency)
		self.touched_account_balances_down.add(key)

	def check_account_modify(self, account):
		return account not in self.touched_accounts

	def commit_account_modify(self, account):
		self.touched_accounts.add(account)

	def check_manage_data(self, account, key):
		if not account in self.managed_data_map.keys():
			return True
		return key in self.managed_data_map[account]

	def commit_manage_data(self, account, key):
		if not account in self.managed_data_map.keys():
			self.managed_data_map[account] = set([])
		self.managed_data_map[account].add(key)

def real_source_account(tx_obj, op):
	if op.source_account is None:
		return tx_obj.sourceAccount
	return demux_account(op.source_account)

def asset_str(asset):
	if asset.type == stellar_xdr.AssetType.ASSET_TYPE_NATIVE:
		return "XLM"
	if asset.type == stellar_xdr.AssetType.ASSET_TYPE_CREDIT_ALPHANUM4:
		return asset.alpha_num4.asset_code.to_xdr()
	return asset.alpha_num12.asset_code.to_xdr()

def asset_code_str(asset):
	if asset.type == stellar_xdr.AssetType.ASSET_TYPE_NATIVE:
		return "XLM"
	if asset.type == stellar_xdr.AssetType.ASSET_TYPE_CREDIT_ALPHANUM4:
		return asset.asset_code4.to_xdr()
	if asset.type == stellar_xdr.AssetType.ASSET_TYPE_CREDIT_ALPHANUM12:
		return asset.asset_code12.to_xdr()
	return "LIQUIDITY_POOL_SHARE"

def change_trust_asset_str(asset):
	if asset.type == stellar_xdr.AssetType.ASSET_TYPE_NATIVE:
		return "XLM"
	if asset.type == stellar_xdr.AssetType.ASSET_TYPE_CREDIT_ALPHANUM4:
		return asset.alpha_num4.asset_code.to_xdr()
	if asset.type == stellar_xdr.AssetType.ASSET_TYPE_CREDIT_ALPHANUM12:
		return asset.alpha_num12.asset_code.to_xdr()
	return "LIQUIDITY_POOL_SHARE"

check_callbacks = {}
commit_callbacks = {}

def check_CREATE_ACCOUNT(conflict_model, tx_obj, op_):
	op = op_.body.create_account_op
	src = real_source_account(tx_obj, op_)

	if not conflict_model.check_account_modify(op.destination.to_xdr()):
		return ConflictStats(stellar_xdr.OperationType.CREATE_ACCOUNT, ConflictReason.ACCOUNT)
	if conflict_model.check_account_balance_down(src, "XLM"):
		return ConflictStats(stellar_xdr.OperationType.CREATE_ACCOUNT, ConflictReason.ACCOUNT_BALANCE)
	return None

def commit_CREATE_ACCOUNT(conflict_model, tx_obj, op_):
	op = op_.body.create_account_op
	src = real_source_account(tx_obj, op_)
	dest = op.destination.to_xdr()#demux_account(op.destination)
	conflict_model.commit_account_modify(dest)
	conflict_model.commit_account_balance_up(dest, "XLM")
	conflict_model.commit_account_balance_down(src, "XLM")

check_callbacks[stellar_xdr.OperationType.CREATE_ACCOUNT] = check_CREATE_ACCOUNT
commit_callbacks[stellar_xdr.OperationType.CREATE_ACCOUNT] = commit_CREATE_ACCOUNT

def check_PAYMENT(conflict_model, tx_obj, op_):
	op = op_.body.payment_op
	asset = asset_str(op.asset)
	target = demux_account(op.destination)
	src = real_source_account(tx_obj, op_)
	if not (conflict_model.check_account_balance_down(src, asset) and conflict_model.check_account_balance_up(target, asset)):
		return ConflictStats(stellar_xdr.OperationType.PAYMENT, ConflictReason.ACCOUNT_BALANCE)
	return None

def commit_PAYMENT(conflict_model, tx_obj, op_):
	op = op_.body.payment_op
	asset = asset_str(op.asset)
	target = demux_account(op.destination)
	src = real_source_account(tx_obj, op_)
	conflict_model.commit_account_balance_down(src, asset) and conflict_model.commit_account_balance_up(target, asset)

check_callbacks[stellar_xdr.OperationType.PAYMENT] = check_PAYMENT
commit_callbacks[stellar_xdr.OperationType.PAYMENT] = commit_PAYMENT





def check_PATH_PAYMENT_STRICT_RECEIVE(conflict_model, tx_obj, op_):
	op = op_.body.path_payment_strict_receive_op
	src = real_source_account(tx_obj, op_)

	sendAsset = asset_str(op.send_asset)
	if not conflict_model.check_account_balance_down(src, sendAsset):
		return ConflictStats(stellar_xdr.OperationType.PATH_PAYMENT_STRICT_RECEIVE, ConflictReason.ACCOUNT_BALANCE)

	for asset in op.path:
		newAsset = asset_str(asset)
		if not conflict_model.check_market(sendAsset, newAsset):
			return ConflictStats(stellar_xdr.OperationType.PATH_PAYMENT_STRICT_RECEIVE, ConflictReason.MARKET)
		sendAsset = newAsset

	destAsset = asset_str(op.dest_asset)

	if not conflict_model.check_market(sendAsset, destAsset):
		return ConflictStats(stellar_xdr.OperationType.PATH_PAYMENT_STRICT_RECEIVE, ConflictReason.MARKET)

	target = demux_account(op.destination)

	if not conflict_model.check_account_balance_up(target, destAsset):
		return ConflictStats(stellar_xdr.OperationType.PATH_PAYMENT_STRICT_RECEIVE, ConflictReason.MARKET)
	return None

def commit_PATH_PAYMENT_STRICT_RECEIVE(conflict_model, tx_obj, op_):
	op = op_.body.path_payment_strict_receive_op
	src = real_source_account(tx_obj, op_)

	sendAsset = asset_str(op.send_asset)
	conflict_model.commit_account_balance_down(src, sendAsset)

	for asset in op.path:
		newAsset = asset_str(asset)
		conflict_model.commit_market(sendAsset, newAsset)
		sendAsset = newAsset

	destAsset = asset_str(op.dest_asset)
	conflict_model.commit_market(sendAsset, destAsset)

	target = demux_account(op.destination)

	conflict_model.commit_account_balance_up(target, destAsset)

check_callbacks[stellar_xdr.OperationType.PATH_PAYMENT_STRICT_RECEIVE] = check_PATH_PAYMENT_STRICT_RECEIVE
commit_callbacks[stellar_xdr.OperationType.PATH_PAYMENT_STRICT_RECEIVE] = commit_PATH_PAYMENT_STRICT_RECEIVE

def check_PATH_PAYMENT_STRICT_SEND(conflict_model, tx_obj, op_):
	op = op_.body.path_payment_strict_send_op
	src = real_source_account(tx_obj, op_)

	sendAsset = asset_str(op.send_asset)
	if not conflict_model.check_account_balance_down(src, sendAsset):
		return ConflictStats(stellar_xdr.OperationType.PATH_PAYMENT_STRICT_SEND, ConflictReason.ACCOUNT_BALANCE)

	for asset in op.path:
		newAsset = asset_str(asset)
		if not conflict_model.check_market(sendAsset, newAsset):
			return ConflictStats(stellar_xdr.OperationType.PATH_PAYMENT_STRICT_SEND, ConflictReason.MARKET)
		sendAsset = newAsset

	destAsset = asset_str(op.dest_asset)

	if not conflict_model.check_market(sendAsset, destAsset):
		return ConflictStats(stellar_xdr.OperationType.PATH_PAYMENT_STRICT_SEND, ConflictReason.MARKET)

	target = demux_account(op.destination)

	if not conflict_model.check_account_balance_up(target, destAsset):
		return ConflictStats(stellar_xdr.OperationType.PATH_PAYMENT_STRICT_SEND, ConflictReason.MARKET)
	return None

def commit_PATH_PAYMENT_STRICT_SEND(conflict_model, tx_obj, op_):
	op = op_.body.path_payment_strict_send_op
	src = real_source_account(tx_obj, op_)

	sendAsset = asset_str(op.send_asset)
	conflict_model.commit_account_balance_down(src, sendAsset)

	for asset in op.path:
		newAsset = asset_str(asset)
		conflict_model.commit_market(sendAsset, newAsset)
		sendAsset = newAsset

	destAsset = asset_str(op.dest_asset)
	conflict_model.commit_market(sendAsset, destAsset)

	target = demux_account(op.destination)

	conflict_model.commit_account_balance_up(target, destAsset)

check_callbacks[stellar_xdr.OperationType.PATH_PAYMENT_STRICT_SEND] = check_PATH_PAYMENT_STRICT_SEND
commit_callbacks[stellar_xdr.OperationType.PATH_PAYMENT_STRICT_SEND] = commit_PATH_PAYMENT_STRICT_SEND

def check_MANAGE_SELL_OFFER(conflict_model, tx_obj, op_):
	op = op_.body.manage_sell_offer_op
	src = real_source_account(tx_obj, op_)

	sellAsset = asset_str(op.selling)
	buyAsset = asset_str(op.buying)

	if not conflict_model.check_market(sellAsset, buyAsset):
		return ConflictStats(stellar_xdr.OperationType.MANAGE_SELL_OFFER, ConflictReason.MARKET)

	if not conflict_model.check_account_balance_down(src, sellAsset):
		return ConflictStats(stellar_xdr.OperationType.MANAGE_SELL_OFFER, ConflictReason.ACCOUNT_BALANCE)
	if not conflict_model.check_account_balance_up(src, sellAsset):
		return ConflictStats(stellar_xdr.OperationType.MANAGE_SELL_OFFER, ConflictReason.ACCOUNT_BALANCE)

	if not conflict_model.check_account_balance_down(src, buyAsset):
		return ConflictStats(stellar_xdr.OperationType.MANAGE_SELL_OFFER, ConflictReason.ACCOUNT_BALANCE)
	if not conflict_model.check_account_balance_up(src, buyAsset):
		return ConflictStats(stellar_xdr.OperationType.MANAGE_SELL_OFFER, ConflictReason.ACCOUNT_BALANCE)

	return None
	
def commit_MANAGE_SELL_OFFER(conflict_model, tx_obj, op_):
	op = op_.body.manage_sell_offer_op
	src = real_source_account(tx_obj, op_)

	sellAsset = asset_str(op.selling)
	buyAsset = asset_str(op.buying)

	conflict_model.commit_market(sellAsset, buyAsset)

	conflict_model.commit_account_balance_down(src, sellAsset)

	conflict_model.commit_account_balance_up(src, sellAsset)

	conflict_model.commit_account_balance_down(src, buyAsset)
	conflict_model.commit_account_balance_up(src, buyAsset)

check_callbacks[stellar_xdr.OperationType.MANAGE_SELL_OFFER] = check_MANAGE_SELL_OFFER
commit_callbacks[stellar_xdr.OperationType.MANAGE_SELL_OFFER] = commit_MANAGE_SELL_OFFER
	
def check_MANAGE_BUY_OFFER(conflict_model, tx_obj, op_):
	op = op_.body.manage_buy_offer_op
	src = real_source_account(tx_obj, op_)

	sellAsset = asset_str(op.selling)
	buyAsset = asset_str(op.buying)

	if not conflict_model.check_market(sellAsset, buyAsset):
		return ConflictStats(stellar_xdr.OperationType.MANAGE_BUY_OFFER, ConflictReason.MARKET)

	if not conflict_model.check_account_balance_down(src, sellAsset):
		return ConflictStats(stellar_xdr.OperationType.MANAGE_BUY_OFFER, ConflictReason.ACCOUNT_BALANCE)
	if not conflict_model.check_account_balance_up(src, sellAsset):
		return ConflictStats(stellar_xdr.OperationType.MANAGE_BUY_OFFER, ConflictReason.ACCOUNT_BALANCE)

	if not conflict_model.check_account_balance_down(src, buyAsset):
		return ConflictStats(stellar_xdr.OperationType.MANAGE_BUY_OFFER, ConflictReason.ACCOUNT_BALANCE)
	if not conflict_model.check_account_balance_up(src, buyAsset):
		return ConflictStats(stellar_xdr.OperationType.MANAGE_BUY_OFFER, ConflictReason.ACCOUNT_BALANCE)

	return None
	
def commit_MANAGE_BUY_OFFER(conflict_model, tx_obj, op_):
	op = op_.body.manage_buy_offer_op
	src = real_source_account(tx_obj, op_)

	sellAsset = asset_str(op.selling)
	buyAsset = asset_str(op.buying)

	conflict_model.commit_market(sellAsset, buyAsset)

	conflict_model.commit_account_balance_down(src, sellAsset)

	conflict_model.commit_account_balance_up(src, sellAsset)

	conflict_model.commit_account_balance_down(src, buyAsset)
	conflict_model.commit_account_balance_up(src, buyAsset)

check_callbacks[stellar_xdr.OperationType.MANAGE_BUY_OFFER] = check_MANAGE_BUY_OFFER
commit_callbacks[stellar_xdr.OperationType.MANAGE_BUY_OFFER] = commit_MANAGE_BUY_OFFER
	
def check_CHANGE_TRUST(conflict_model, tx_obj, op_):
	op = op_.body.change_trust_op
	src = real_source_account(tx_obj, op_)

	line = op.line
	asset = change_trust_asset_str(line)
	if not (conflict_model.check_account_balance_down(src, asset) and conflict_model.check_account_balance_up(src, asset)):
		return ConflictStats(stellar_xdr.OperationType.CHANGE_TRUST, ConflictReason.ACCOUNT_BALANCE)

def commit_CHANGE_TRUST(conflict_model, tx_obj, op_):
	op = op_.body.change_trust_op
	src = real_source_account(tx_obj, op_)
	line = op.line
	asset = change_trust_asset_str(line)
	conflict_model.commit_account_balance_down(src, asset)
	conflict_model.commit_account_balance_up(src, asset)

check_callbacks[stellar_xdr.OperationType.CHANGE_TRUST] = check_CHANGE_TRUST
commit_callbacks[stellar_xdr.OperationType.CHANGE_TRUST] = commit_CHANGE_TRUST

def check_MANAGE_DATA(conflict_model, tx_obj, op_):
	op = op_.body.manage_data_op
	src = real_source_account(tx_obj, op_)
	if not conflict_model.check_manage_data(src, op.data_name.to_xdr()):
		return ConflictStats(stellar_xdr.OperationType.MANAGE_DATA, ConflictReason.ACCOUNT)

def commit_MANAGE_DATA(conflict_model, tx_obj, op_):
	op = op_.body.manage_data_op
	src = real_source_account(tx_obj, op_)
	conflict_model.commit_manage_data(src, op.data_name.to_xdr())

check_callbacks[stellar_xdr.OperationType.MANAGE_DATA] = check_MANAGE_DATA
commit_callbacks[stellar_xdr.OperationType.MANAGE_DATA] = commit_MANAGE_DATA

def check_ACCOUNT_MERGE(conflict_model, tx_obj, op_):
	src = real_source_account(tx_obj, op_)
	dest = demux_account(op_.body.destination)
	if not (conflict_model.check_account_modify(src) and conflict_model.check_account_modify(dest)):
		return ConflictStats(stellar_xdr.OperationType.ACCOUNT_MERGE, ConflictReason.ACCOUNT)

def commit_ACCOUNT_MERGE(conflict_model, tx_obj, op_):
	src = real_source_account(tx_obj, op_)
	dest = demux_account(op_.body.destination)
	conflict_model.commit_account_modify(src)
	conflict_model.commit_account_modify(dest)

check_callbacks[stellar_xdr.OperationType.ACCOUNT_MERGE] = check_ACCOUNT_MERGE
commit_callbacks[stellar_xdr.OperationType.ACCOUNT_MERGE] = commit_ACCOUNT_MERGE


def check_ALLOW_TRUST(conflict_model, tx_obj, op_):
	op = op_.body.allow_trust_op
	src = real_source_account(tx_obj, op_)

	asset = asset_code_str(op.asset)

	if not (conflict_model.check_account_balance_down(src, asset) and conflict_model.check_account_balance_up(src, asset)):
		return ConflictStats(stellar_xdr.OperationType.ALLOW_TRUST, ConflictReason.ACCOUNT_BALANCE)

def commit_ALLOW_TRUST(conflict_model, tx_obj, op_):
	op = op_.body.allow_trust_op
	src = real_source_account(tx_obj, op_)
	asset = asset_code_str(op.asset)

	conflict_model.commit_account_balance_down(src, asset)
	conflict_model.commit_account_balance_up(src, asset)

check_callbacks[stellar_xdr.OperationType.ALLOW_TRUST] = check_ALLOW_TRUST
commit_callbacks[stellar_xdr.OperationType.ALLOW_TRUST] = commit_ALLOW_TRUST


def check_operation(tx_obj, conflict_model, op):
	if op.body.type in check_callbacks.keys():
		return check_callbacks[op.body.type](conflict_model, tx_obj, op)
	else:
		print ("unknown op caused fail", op.body.type)
		return ConflictStats(op.body.type, ConflictReason.UNKNOWN)

def commit_operation(tx_obj, conflict_model, op):
	if op.body.type in commit_callbacks.keys():
		commit_callbacks[op.body.type](conflict_model, tx_obj, op)



def check_tx(tx_obj, conflict_model):
	if not conflict_model.check_account_balance_down(tx_obj.feeAccount, "XLM"):
		return ConflictStats(None, ConflictReason.ACCOUNT_BALANCE)

	for op in tx_obj.operations:
		res = check_operation(tx_obj, conflict_model, op)
		if res is not None:
			return res

	return None

def commit_tx(tx_obj, conflict_model):
	conflict_model.commit_account_balance_down(tx_obj.feeAccount, "XLM")

	for op in tx_obj.operations:
		commit_operation(tx_obj, conflict_model, op)


def conflict_analyse(tx_list):
	model = ConflictModel()

	successes = 0
	fails = 0

	stats = ConflictStats(None, None, empty=True)

	for tx in tx_list:
		res = check_tx(tx, model)
		if res is None:
			successes += 1
		else:
			stats.add(res)
			fails += 1
		commit_tx(tx, model)

	#print(successes, fails)
	return (successes, fails, stats)

import matplotlib.pyplot as plt
from tqdm import tqdm

def agg_analyse(start, end):
	fracs = []

	overall_stats = ConflictStats(None, None, empty=True)

	for i in tqdm(range(start, end)):
		res = get_transactions(i)
		(good, bad, stats) = conflict_analyse(res)
		fracs.append(float(good) / float(good + bad))
		overall_stats.add(stats)

	plt.hist(fracs)
	plt.savefig("histogram2.png")

	for conflict in overall_stats.conflicts:
		print (conflict, overall_stats.conflicts[conflict])

	for reason in overall_stats.reasons:
		print(reason, overall_stats.reasons[reason])

	plt.show()


agg_analyse(36557164, 36557264)









