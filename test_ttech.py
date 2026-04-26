#!/usr/bin/env python3
"""Test script for T-Tech Investments API - Sync version"""

import os
from datetime import datetime, timedelta, timezone

# Set the token
os.environ['INVEST_TOKEN'] = 't.ZLtpCN0pOiGj8WbOU0xxGgpCWxBH5vmnYH-hzvgXQesS04yGMtEiw1tzJevGZox1r6nVMXi0z0QMO3BaRH7lBA'

def test_ttech():
    from t_tech.invest import Client, CandleInterval
    from t_tech.invest.schemas import GetCandlesRequest
    
    print("✓ T-Tech module imported successfully")
    
    token = os.environ.get('INVEST_TOKEN')
    if not token:
        print("✗ INVEST_TOKEN not set")
        return
        
    with Client(token=token) as client:
        print("✓ Client created successfully")
        
        # Find SNGS by ticker first to get the correct FIGI
        print("\nSearching for SNGS by ticker...")
        shares_response = client.instruments.shares()
        sngs = None
        for share in shares_response.instruments:
            if share.ticker == 'SNGS':
                sngs = share
                break
        
        if not sngs:
            print("✗ SNGS not found")
            return
            
        figi = sngs.figi
        print(f"✓ Found SNGS: {sngs.name} (FIGI: {figi}, Ticker: {sngs.ticker})")
        
        # Get candles
        print(f"\nFetching candles for {figi} (SNGS)...")
        now = datetime.now(timezone.utc)
        from_time = now - timedelta(days=7)
        
        candles_response = client.market_data.get_candles(
            figi=figi,
            interval=CandleInterval.CANDLE_INTERVAL_4_HOUR,
            from_=from_time,
            to=now
        )
        candles = candles_response.candles
        print(f"✓ Retrieved {len(candles)} candles")
        
        if candles:
            print("\nFirst 5 candles:")
            for i, candle in enumerate(candles[:5]):
                print(f"  {i+1}. Time: {candle.time}, Open: {candle.open}, Close: {candle.close}, Volume: {candle.volume}")
            
            print("\nLast 5 candles:")
            for i, candle in enumerate(candles[-5:]):
                idx = len(candles) - 5 + i + 1
                print(f"  {idx}. Time: {candle.time}, Open: {candle.open}, Close: {candle.close}, Volume: {candle.volume}")
            
            # Check if sorted correctly
            times = [c.time for c in candles]
            is_sorted = all(times[i] <= times[i+1] for i in range(len(times)-1))
            print(f"\n✓ Candles are sorted chronologically: {is_sorted}")
            
            if not is_sorted:
                print("⚠ WARNING: Candles are NOT sorted! This may cause issues.")
        
        print("\n✓ Test completed successfully!")

if __name__ == '__main__':
    try:
        test_ttech()
    except Exception as e:
        print(f"✗ Error: {e}")
        import traceback
        traceback.print_exc()
