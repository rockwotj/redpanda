
# Redpanda Golang WASM Transform

To get started you first need to have installed [tinygo].

You can get started by modifying the <code>transform.go</code> file
with your logic.

Once you're ready to test out your transform live you need to:

1. Make sure you have a container running via <code>rpk container start</code>
1. Run <code>rpk wasm build</code>
1. Run <code>rpk wasm deploy [topic]</code>
1. Then use <code>rpk topic produce [topic]</code> and <code>rpk topic consume [topic]</code> 
   to see your transformation live!

⚠️ At the moment the transform you deploy is applied on all topics in a cluster 
and is not persisted to disk, so if you restart your container you'll need to redeploy.

[tinygo]: https://tinygo.org/getting-started/install/
