# kick
> A dummy background job system inspired by [sidekiq](https://github.com/mperham/sidekiq) and [go-workers](https://github.com/jrallison/go-workers)


## Support Features
* Retry
* Schedule Job


## TODO
- [ ] Retries with exponential backoff
- [x] Store job in Redis
- [ ] Add tests
- [x] Support retry
- [x] shutdown kick server gracefully
- [ ] Prometheus stats endpoint
- [ ] Fancy UI
- [ ] different concurrency per queue 