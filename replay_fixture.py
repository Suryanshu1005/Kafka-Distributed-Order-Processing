#!/usr/bin/env python3
"""Replay fixture orders through the live Order Service."""

import json
import requests
import sys
import time
from datetime import datetime


def load_fixture():
    """Load orders from fixture file."""
    with open('sample-data/orders_api_payloads.json') as f:
        return json.load(f)

def transform_order(order):
    """Transform fixture order to API payload format."""
    return {
        'customer_id': order.get('order_id', 'UNKNOWN'),  # Use order_id as customer_id for now
        'customer_email': order['customer']['email'],
        'items': [
            {
                'product_id': str(item['product_id']),  # Ensure string
                'quantity': item['quantity'],
                'unit_price': item['unit_price']
            }
            for item in order['items']
        ]
    }

def replay_orders(base_url='http://localhost:8000'):
    """Send all fixture orders to the Order Service."""
    orders = load_fixture()
    expected = len(orders)
    print('=' * 70)
    print(f'FIXTURE REPLAY - {len(orders)} ORDERS')
    print('=' * 70)
    print(f'Start Time: {datetime.now().isoformat()}\n')
    
    results = {
        'success': [],
        'failed': [],
        'timeouts': []
    }
    
    for idx, order in enumerate(orders, 1):
        try:
            payload = transform_order(order)
            response = requests.post(
                f'{base_url}/orders',
                json=payload,
                timeout=10
            )
            
            if response.status_code == 201:
                result = response.json()
                results['success'].append({
                    'order_id': result['order_id'],
                    'correlation_id': result['correlation_id'],
                    'timestamp': result['timestamp']
                })
                print(f'[{idx:2d}/{expected}] OK {result["order_id"][:20]}... -> correlation: {result["correlation_id"][:8]}...')
            else:
                results['failed'].append({
                    'fixture_order': order.get('order_id'),
                    'status_code': response.status_code,
                    'error': response.text
                })
                print(f'[{idx:2d}/{expected}] FAIL Status {response.status_code}')
                
        except requests.exceptions.Timeout:
            results['timeouts'].append({'fixture_order': order.get('order_id'), 'error': 'timeout'})
            print(f'[{idx:2d}/{expected}] TIMEOUT')
        except Exception as e:
            results['failed'].append({'fixture_order': order.get('order_id'), 'error': str(e)})
            print(f'[{idx:2d}/{expected}] FAIL {str(e)[:50]}')
        
        # Small delay between requests
        time.sleep(0.1)
    
    # Print summary
    print(f'\n' + '=' * 70)
    print(f'REPLAY SUMMARY')
    print(f'=' * 70)
    print(f'Success: {len(results["success"])}/{expected}')
    print(f'Failed: {len(results["failed"])}/{expected}')
    print(f'Timeouts: {len(results["timeouts"])}/{expected}')
    print(f'End Time: {datetime.now().isoformat()}')
    print(f'=' * 70)
    
    if len(results['success']) == expected:
        print(f'OK FIXTURE REPLAY COMPLETE - ALL {expected} ORDERS ACCEPTED')
        return 0
    else:
        print(f'FAIL FIXTURE REPLAY INCOMPLETE - {expected - len(results["success"])} orders not accepted')
        return 1

if __name__ == '__main__':
    sys.exit(replay_orders())
