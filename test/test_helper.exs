:ok = LocalCluster.start()

path = Path.expand("..", __ENV__.file)
Code.compile_file("cluster/nodes.ex", path)
Code.compile_file("cluster/socket_plug_node.ex", path)

ExUnit.start()
