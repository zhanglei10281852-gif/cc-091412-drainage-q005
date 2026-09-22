from app import create_server


if __name__ == "__main__":
    server = create_server()
    print("排水抢修调度服务已启动", flush=True)
    server.serve_forever()
