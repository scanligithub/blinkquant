# 2. 串行流式下载和解析，严格控制单次内存开销
            # 引入指数退避重试，防止由于 CDN 闪断导致节点初始化失败
            async with httpx.AsyncClient(headers=headers, follow_redirects=True, timeout=60.0) as client:
                for fname in data_files:
                    logger.info(f"Node {self.node_index}: Loading {fname}...")
                    url = base_url + fname
                    
                    content = None
                    # 指数退避重试 3 次
                    for attempt in range(1, 4):
                        try:
                            response = await client.get(url)
                            response.raise_for_status()
                            content = response.content
                            break # 下载成功，跳出重试
                        except Exception as download_err:
                            if attempt == 3:
                                logger.error(f"Node {self.node_index}: Failed to download {fname} after 3 attempts: {download_err}")
                            else:
                                wait_time = attempt * 3 # 分别等待 3s, 6s 重试
                                logger.warning(f"Node {self.node_index}: Temp download error for {fname} ({download_err}). Retrying in {wait_time}s...")
                                await asyncio.sleep(wait_time)
                    
                    # 如果重试 3 次后该文件依然下载失败，为保证数据完整性，不应强行启动（否则可能导致空数据错乱）
                    if content is None:
                        raise ValueError(f"Core data file {fname} failed to load. Aborting initialization to force safer redeploy.")
                    
                    bio = io.BytesIO(content)
                    
                    # 根据文件名类型分类解析，并在处理完成后立即 del 释放内存
                    if "stock_list.parquet" in fname:
                        sdf = pl.read_parquet(bio)
                        self.code_to_name = {row[0]: row[1] for row in sdf.select(["code", "code_name"]).iter_rows()}
                        del sdf
                    
                    elif "stock_kline_" in fname:
                        df = pl.read_parquet(bio)
                        node_filter = (df["code"].hash() % self.total_nodes) == self.node_index
                        sharded_df = df.filter(node_filter)
                        if not sharded_df.is_empty():
                            kline_dfs.append(sharded_df)
                        del df
                        
                    elif "stock_money_flow_" in fname:
                        df = pl.read_parquet(bio)
                        node_filter = (df["code"].hash() % self.total_nodes) == self.node_index
                        sharded_flow = df.filter(node_filter)
                        if not sharded_flow.is_empty():
                            flow_dfs.append(sharded_flow)
                        del df
                        
                    elif "sector_kline_" in fname:
                        sdf = pl.read_parquet(bio)
                        sector_dfs.append(sdf)
                    
                    # 强力垃圾回收，防止字节流在堆中残留
                    del content
                    del bio
                    gc.collect()
