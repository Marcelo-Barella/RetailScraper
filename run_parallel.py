#!/usr/bin/env python
"""
Parallel execution script for maximum scraping performance.
Runs multiple spider instances concurrently.
"""

import subprocess
import threading
import time
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
import json
from helpers.helpers import cleanup_temp_directories


def split_stores(stores_file, num_splits):
    """Split stores into chunks for parallel processing"""
    stores = []
    with open(stores_file, 'r') as f:
        for line in f:
            store = json.loads(line.strip())
            if store.get('store_id'):
                stores.append(store)
    
    chunk_size = len(stores) // num_splits
    chunks = []
    
    for i in range(num_splits):
        start_idx = i * chunk_size
        end_idx = start_idx + chunk_size if i < num_splits - 1 else len(stores)
        chunk = stores[start_idx:end_idx]
        
        # Save chunk to temporary file
        chunk_file = f'data/stores_chunk_{i}.jl'
        with open(chunk_file, 'w') as f:
            for store in chunk:
                f.write(json.dumps(store) + '\n')
        
        chunks.append({
            'id': i,
            'file': chunk_file,
            'count': len(chunk),
            'start': chunk[0]['store_id'] if chunk else None,
            'end': chunk[-1]['store_id'] if chunk else None
        })
    
    return chunks


def run_spider_instance(chunk_info, spider_name='walmart_products', threads=50):
    """Run a single spider instance for a chunk of stores"""
    chunk_id = chunk_info['id']
    stores_file = chunk_info['file']
    
    print(f"[Instance {chunk_id}] Starting spider for {chunk_info['count']} stores "
          f"({chunk_info['start']} to {chunk_info['end']})")
    
    cmd = [
        'python', 'main.py',
        f'--scrape-{spider_name}',
        '--threads', str(threads),
        '--stores-file', stores_file,
        '--output-suffix', f'_chunk_{chunk_id}'
    ]
    
    try:
        # Run the spider
        process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )
        
        # Monitor output
        for line in process.stdout:
            if 'Scraped' in line or 'ERROR' in line or 'finished' in line:
                print(f"[Instance {chunk_id}] {line.strip()}")
        
        process.wait()
        
        if process.returncode == 0:
            print(f"[Instance {chunk_id}] Completed successfully")
        else:
            print(f"[Instance {chunk_id}] Failed with code {process.returncode}")
            
        return chunk_id, process.returncode
        
    except Exception as e:
        print(f"[Instance {chunk_id}] Error: {e}")
        return chunk_id, -1


def merge_results(num_chunks, output_pattern='data/products_chunk_{}.jl'):
    """Merge results from all chunks into a single file"""
    print("\nMerging results...")
    
    with open('data/products_merged.jl', 'w') as outfile:
        total_items = 0
        for i in range(num_chunks):
            chunk_file = output_pattern.format(i)
            if os.path.exists(chunk_file):
                with open(chunk_file, 'r') as infile:
                    for line in infile:
                        outfile.write(line)
                        total_items += 1
                
                # Clean up chunk file
                os.remove(chunk_file)
        
    print(f"Merged {total_items} items into data/products_merged.jl")


def main():
    """Main parallel execution function"""
    import argparse
    
    # --- Start by cleaning up temp directories from previous runs ---
    cleanup_temp_directories()
    
    parser = argparse.ArgumentParser(description='Run spiders in parallel for maximum performance')
    parser.add_argument('--spider', default='products', choices=['stores', 'categories', 'products'],
                        help='Which spider to run')
    parser.add_argument('--instances', type=int, default=3,
                        help='Number of parallel instances to run')
    parser.add_argument('--threads-per-instance', type=int, default=50,
                        help='Number of threads per spider instance')
    parser.add_argument('--stores-file', default='data/stores.jl',
                        help='Input stores file')
    
    args = parser.parse_args()
    
    print(f"=== Parallel Scraping Configuration ===")
    print(f"Spider: {args.spider}")
    print(f"Parallel instances: {args.instances}")
    print(f"Threads per instance: {args.threads_per_instance}")
    print(f"Total concurrent requests: {args.instances * args.threads_per_instance}")
    print("=====================================\n")
    
    if args.spider == 'products':
        # Split stores for parallel processing
        print("Splitting stores into chunks...")
        chunks = split_stores(args.stores_file, args.instances)
        
        # Run spider instances in parallel
        print(f"\nStarting {args.instances} spider instances...\n")
        
        with ThreadPoolExecutor(max_workers=args.instances) as executor:
            futures = []
            for chunk in chunks:
                future = executor.submit(
                    run_spider_instance,
                    chunk,
                    args.spider,
                    args.threads_per_instance
                )
                futures.append(future)
            
            # Wait for all instances to complete
            for future in as_completed(futures):
                chunk_id, returncode = future.result()
                if returncode != 0:
                    print(f"Warning: Instance {chunk_id} failed")
        
        # Merge results
        merge_results(args.instances)
        
        # Clean up temporary chunk files
        for chunk in chunks:
            if os.path.exists(chunk['file']):
                os.remove(chunk['file'])
    
    else:
        # For stores and categories, just run with high thread count
        cmd = [
            'python', 'main.py',
            f'--find-{args.spider}',
            '--threads', str(args.instances * args.threads_per_instance)
        ]
        
        print(f"Running: {' '.join(cmd)}")
        subprocess.run(cmd)
    
    print("\n=== Parallel execution complete ===")


if __name__ == '__main__':
    main() 