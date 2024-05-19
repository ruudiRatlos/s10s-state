# s10s-state


## Release

```bash
export TAG=v1.0.x
git tag $TAG
git push && git push origin $TAG
GOPROXY=proxy.golang.org go list -m github.com/ruudiRatlos/s10s-state@$TAG
```
