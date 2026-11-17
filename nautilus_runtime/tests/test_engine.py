from decimal import Decimal as D
from qd_arbitrage.contracts import defaults
from qd_nautilus.engine import build_engine, native_event, MemorySink, common_step

T = 1793491200000000000
RULES = {v: {'symbol': 'BTCUSDT' if v=='BINANCE' else 'BTC-USDT', 'tick':'0.01','step':'0.00001','minQty':'0.00001','minNotional':'5'} for v in ['BINANCE','OKX']}


def book(venue, ms, bid, ask, quantity='1', snapshot=True):
    return dict(kind='book',venue=venue,ts=T+ms*1000000,eventNs=T+ms*1000000,sequence=ms+1,snapshot=snapshot,
                levels=[('bid',str(bid),quantity),('ask',str(ask),quantity)])


def run(events, config=None):
    config = config or defaults()
    sink = MemorySink()
    engine,strategy,instruments=build_engine(config,RULES,sink)
    engine.add_data([native_event(e,instruments) for e in events])
    engine.run()
    result=strategy.report()
    assert strategy.error is None
    return result,sink


def test_native_ioc_quote_fees_and_independent_cash():
    events=[book('BINANCE',0,59999,60000),book('OKX',0,60300,60301),
            book('BINANCE',100,59999,60000),book('OKX',100,60300,60301),
            book('BINANCE',200,59999,60000),book('OKX',200,60300,60301)]
    cfg=defaults();cfg['risk']['maxCycles']=1
    result,sink=run(events,cfg)
    assert result['completedCycles']==1
    assert D(result['realizedProfit'])>0
    assert D(result['balances']['BINANCE']['BTC'])>D('.1')
    assert D(result['balances']['OKX']['BTC'])<D('.1')
    assert D(result['exposureBase'])==0
    assert len(sink.cycles[1]['legs'])==2
    assert all(x['terminal'] for x in sink.cycles[1]['legs'])


def test_no_inventory_means_no_sell_and_no_borrowing():
    cfg=defaults();cfg['venues']['OKX']['baseBalance']='0'
    result,sink=run([book('BINANCE',0,59999,60000),book('OKX',0,60300,60301),book('OKX',100,60300,60301)],cfg)
    assert result['cycles']==0


def test_common_step_is_lcm():
    assert common_step('.002','.003')==D('.006')


def test_series_snapshot_uses_monotonic_sequences_and_real_drawdown():
    sink = MemorySink()
    engine, strategy, _ = build_engine(defaults(), RULES, sink)
    first = strategy.series_snapshot({'asOfNs': T, 'equity': '10010', 'exposureQuote': '2', 'fees': '1', 'drawdown': '0'})
    second = strategy.series_snapshot({'asOfNs': T, 'equity': '9990', 'exposureQuote': '4', 'fees': '2', 'drawdown': '-20'})
    assert [point['seq'] for point in first if point['metric'] == 'equity'] == [1]
    assert [point['seq'] for point in second if point['metric'] == 'equity'] == [2]
    assert next(point for point in second if point['metric'] == 'drawdown')['value'] == '-20'
    assert all(point['tsNs'] == T for point in first + second)
    engine.end()


def scenario(sell_quantity='1', compensation_liquidity='1'):
    cfg=defaults();cfg['risk']['maxCycles']=1
    cfg['venues']['BINANCE']['latencyMs']=10
    cfg['venues']['OKX']['latencyMs']=200
    events=[book('BINANCE',0,59999,60000),book('OKX',0,60300,60301),
            book('BINANCE',50,59999,60000),
            book('OKX',100,59000 if sell_quantity=='0' else 60300,60301, sell_quantity if sell_quantity!='0' else '1'),
            book('BINANCE',250,59999,60000,compensation_liquidity),
            book('OKX',300,59000,59001),book('BINANCE',350,59999,60000,compensation_liquidity),
            book('BINANCE',450,59999,60000,compensation_liquidity)]
    return run(events,cfg)


def test_one_leg_misses_compensates_confirmed_fill_once():
    result,sink=scenario('0')
    cycle=sink.cycles[1]
    assert cycle['state']=='completed'
    assert cycle['compensationAttempted']
    assert len(cycle['legs'])==3
    assert D(result['realizedProfit'])<0
    assert D(result['exposureBase'])==0
    assert cycle['legs'][1]['filled']=='0'


def test_partial_fill_compensates_actual_difference():
    result,sink=scenario('0.00050')
    cycle=sink.cycles[1]
    assert len(cycle['legs'])==3
    buy,sell,comp=cycle['legs']
    assert D(comp['planned'])==D(buy['filled'])-D(sell['filled'])
    assert D(result['exposureBase'])==0
    assert result['partialFillRate']=='1'


def test_compensation_depth_failure_pauses_and_retains_exposure():
    result,sink=scenario('0','0.00001')
    assert result['pausedReason']=='COMPENSATION_INSUFFICIENT_DEPTH'
    assert sink.cycles[1]['state']=='manual'
    assert D(result['exposureBase'])>0
    assert sink.cycles[1]['realizedProfit'] is None


def test_equity_unavailable_with_stale_quotes():
    result,_=run([book('BINANCE',0,59999,60000),book('OKX',2000,60000,60001)])
    assert not result['valuationAvailable']
    assert result['equity'] is None


def test_repeated_native_replay_economic_events_identical():
    cfg=defaults();cfg['risk']['maxCycles']=1
    events=[book('BINANCE',0,59999,60000),book('OKX',0,60300,60301),book('BINANCE',100,59999,60000),book('OKX',200,60300,60301)]
    a,sa=run(events,cfg);b,sb=run(events,cfg)
    assert a==b
    assert sa.cycles==sb.cycles
    assert sa.events==sb.events


def test_funds_cannot_be_pooled_between_venues():
    cfg=defaults();cfg['venues']['BINANCE']['quoteBalance']='0';cfg['venues']['OKX']['quoteBalance']='999999'
    result,_=run([book('BINANCE',0,59999,60000),book('OKX',0,60300,60301)],cfg)
    assert result['cycles']==0


def test_dust_retained_in_exposure_and_profit_not_fabricated():
    result,sink=scenario('0.00165')
    assert result['pausedReason']=='DUST_REQUIRES_HANDLING'
    assert D(result['dustBase'])==D('0.00001')
    assert D(result['exposureBase'])==D('0.00001')
    assert sink.cycles[1]['realizedProfit'] is None


def test_native_risk_denial_of_sell_leg_still_compensates_buy():
    cfg=defaults();cfg['risk']['maxCycles']=1;cfg['venues']['OKX']['baseBalance']='0'
    sink=MemorySink();engine,strategy,instruments=build_engine(cfg,RULES,sink)
    original=strategy.free
    # Inject a stale preflight balance while the native account has no inventory.
    strategy.free=lambda venue,currency: D('1') if venue=='OKX' and currency=='BTC' else original(venue,currency)
    events=[book('BINANCE',0,59999,60000),book('OKX',0,60300,60301),
            book('BINANCE',100,59999,60000),book('OKX',200,60300,60301),book('BINANCE',300,59999,60000),book('BINANCE',400,59999,60000)]
    engine.add_data([native_event(e,instruments) for e in events]);engine.run()
    assert any(e['type'] in ('OrderDenied','OrderRejected') for e in sink.events)
    assert sink.cycles[1]['compensationAttempted']
    assert D(strategy.report()['exposureBase'])==0


def test_stop_cancels_pending_and_preserves_prefunded_inventory():
    cfg=defaults();cfg['venues']['BINANCE']['latencyMs']=200;cfg['venues']['OKX']['latencyMs']=200
    sink=MemorySink();engine,strategy,instruments=build_engine(cfg,RULES,sink,control=lambda:True)
    events=[book('BINANCE',0,59999,60000),book('OKX',0,60300,60301),book('BINANCE',100,59999,60000),book('OKX',300,60300,60301),book('BINANCE',400,59999,60000)]
    engine.add_data([native_event(e,instruments) for e in events]);engine.run()
    result=strategy.report()
    assert strategy.stopping
    assert D(result['balances']['BINANCE']['BTC'])+D(result['balances']['OKX']['BTC'])==D('.2')
    assert all(l['terminal'] for c in sink.cycles.values() for l in c['legs'])


def test_streaming_replay_matches_single_batch():
    cfg=defaults();cfg['risk']['maxCycles']=1
    events=[book('BINANCE',0,59999,60000),book('OKX',0,60300,60301),book('BINANCE',100,59999,60000),book('OKX',200,60300,60301)]
    expected,baseline=run(events,cfg)
    sink=MemorySink();engine,strategy,instruments=build_engine(cfg,RULES,sink)
    for chunk in [events[:2],events[2:]]:
        engine.add_data([native_event(e,instruments) for e in chunk]);engine.run(streaming=True);engine.clear_data()
    strategy.advance();engine.end()
    assert strategy.report()==expected
    assert sink.cycles==baseline.cycles


def test_known_spread_exact_cash_and_fee_ledger():
    cfg=defaults();cfg['risk']['maxCycles']=1
    result,sink=run([book('BINANCE',0,59999,60000),book('OKX',0,60300,60301),book('BINANCE',100,59999,60000),book('OKX',200,60300,60301)],cfg)
    assert D(result['realizedProfit'])==D('0.298302')
    assert D(result['fees'])==D('0.199698')
    assert D(result['balances']['BINANCE']['USDT'])==D('9900.3004')
    assert D(result['balances']['OKX']['USDT'])==D('10099.997902')
    assert [D(leg['vwap']) for leg in sink.cycles[1]['legs']]==[D('60000'),D('60300')]


def test_event_sink_failure_is_not_silently_swallowed_by_native_callback():
    cfg=defaults();cfg['risk']['maxCycles']=1
    class FailingSink(MemorySink):
        def event(self,body): raise OSError('test storage failure')
    engine,strategy,instruments=build_engine(cfg,RULES,FailingSink())
    engine.add_data([native_event(e,instruments) for e in [book('BINANCE',0,59999,60000),book('OKX',0,60300,60301),book('OKX',200,60300,60301)]])
    engine.run()
    assert 'test storage failure' in strategy.error
    assert strategy.stopping


def test_equal_profit_directions_choose_fixed_binance_buy_order():
    cfg=defaults();sink=MemorySink();engine,strategy,instruments=build_engine(cfg,RULES,sink)
    selected=[]
    strategy.opportunity=lambda buy,sell: {'buy':buy,'sell':sell,'profit':D('1')}
    strategy.begin=selected.append
    engine.add_data([native_event(book('BINANCE',0,59999,60000),instruments)])
    engine.run()
    assert selected[0]['buy']=='BINANCE'
