// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use std::num::NonZeroU32;
use std::sync::Arc;
use std::thread;

use governor::Quota;
use governor::RateLimiter;
use governor::clock::Clock;
use governor::clock::DefaultClock;
use governor::middleware::NoOpMiddleware;
use governor::state::InMemoryState;
use governor::state::NotKeyed;

use opendal::raw::*;
use opendal::*;

/// Add a bandwidth rate limiter to the underlying services.
///
/// # Throttle
///
/// There are several algorithms when it come to rate limiting techniques.
/// This throttle layer uses Generic Cell Rate Algorithm (GCRA) provided by
/// [Governor](https://docs.rs/governor/latest/governor/index.html).
/// By setting the `bandwidth` and `burst`, we can control the byte flow rate of underlying services.
///
/// # Note
///
/// When setting the ThrottleLayer, always consider the largest possible operation size as the burst size,
/// as **the burst size should be larger than any possible byte length to allow it to pass through**.
///
/// Read more about [Quota](https://docs.rs/governor/latest/governor/struct.Quota.html#examples)
///
/// # Examples
///
/// This example limits bandwidth to 10 KiB/s and burst size to 10 MiB.
///
/// ```no_run
/// # use opendal::layers::ThrottleLayer;
/// # use opendal::services;
/// # use opendal::Operator;
/// # use opendal::Result;
/// # use opendal::Scheme;
///
/// # fn main() -> Result<()> {
/// let _ = Operator::new(services::Memory::default())
///     .expect("must init")
///     .layer(ThrottleLayer::new(10 * 1024, 10000 * 1024))
///     .finish();
/// Ok(())
/// # }
/// ```
#[derive(Clone)]
pub struct ThrottleLayer {
    reqs_per_minute: NonZeroU32,
}

impl ThrottleLayer {
    pub fn new(reqs_per_minute: u32) -> Self {
        Self {
            reqs_per_minute: NonZeroU32::new(reqs_per_minute as _).unwrap(),
        }
    }
}

impl<A: Access> Layer<A> for ThrottleLayer {
    type LayeredAccess = ThrottleAccessor<A>;

    fn layer(&self, accessor: A) -> Self::LayeredAccess {
        let rate_limiter = Arc::new(RateLimiter::direct(Quota::per_minute(self.reqs_per_minute)));
        ThrottleAccessor {
            inner: accessor,
            rate_limiter,
        }
    }
}

/// Share an atomic RateLimiter instance across all threads in one operator.
/// If want to add more observability in the future, replace the default NoOpMiddleware with other middleware types.
/// Read more about [Middleware](https://docs.rs/governor/latest/governor/middleware/index.html)
type SharedRateLimiter = Arc<RateLimiter<NotKeyed, InMemoryState, DefaultClock, NoOpMiddleware>>;

#[derive(Debug, Clone)]
pub struct ThrottleAccessor<A: Access> {
    inner: A,
    rate_limiter: SharedRateLimiter,
}

impl<A: Access> LayeredAccess for ThrottleAccessor<A> {
    type Inner = A;
    type Reader = ThrottleWrapper<A::Reader>;
    type Writer = A::Writer;
    type Lister = ThrottleWrapper<A::Lister>;
    type Deleter = A::Deleter;
    type BlockingReader = ThrottleWrapper<A::BlockingReader>;
    type BlockingWriter = A::BlockingWriter;
    type BlockingLister = ThrottleWrapper<A::BlockingLister>;
    type BlockingDeleter = A::BlockingDeleter;

    fn inner(&self) -> &Self::Inner {
        &self.inner
    }

    async fn read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::Reader)> {
        let limiter = self.rate_limiter.clone();

        self.inner
            .read(path, args)
            .await
            .map(|(rp, r)| (rp, ThrottleWrapper::new(r, limiter)))
    }

    async fn write(&self, path: &str, args: OpWrite) -> Result<(RpWrite, Self::Writer)> {
        self.inner.write(path, args).await
    }

    async fn delete(&self) -> Result<(RpDelete, Self::Deleter)> {
        self.inner.delete().await
    }

    async fn list(&self, path: &str, args: OpList) -> Result<(RpList, Self::Lister)> {
        let limiter = self.rate_limiter.clone();

        self.inner
            .list(path, args)
            .await
            .map(|(rp, r)| (rp, ThrottleWrapper::new(r, limiter)))
    }

    fn blocking_read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::BlockingReader)> {
        let limiter = self.rate_limiter.clone();

        self.inner
            .blocking_read(path, args)
            .map(|(rp, r)| (rp, ThrottleWrapper::new(r, limiter)))
    }

    fn blocking_write(&self, path: &str, args: OpWrite) -> Result<(RpWrite, Self::BlockingWriter)> {
        self.inner.blocking_write(path, args)
    }

    fn blocking_delete(&self) -> Result<(RpDelete, Self::BlockingDeleter)> {
        self.inner.blocking_delete()
    }

    fn blocking_list(&self, path: &str, args: OpList) -> Result<(RpList, Self::BlockingLister)> {
        let limiter = self.rate_limiter.clone();

        self.inner
            .blocking_list(path, args)
            .map(|(rp, r)| (rp, ThrottleWrapper::new(r, limiter)))
    }
}

pub struct ThrottleWrapper<R> {
    inner: R,
    limiter: SharedRateLimiter,
}

impl<R> ThrottleWrapper<R> {
    pub fn new(inner: R, rate_limiter: SharedRateLimiter) -> Self {
        Self {
            inner,
            limiter: rate_limiter,
        }
    }
}

impl<R: oio::Read> oio::Read for ThrottleWrapper<R> {
    async fn read(&mut self) -> Result<Buffer> {
        self.limiter.until_ready().await;
        self.inner.read().await
    }
}

impl<R: oio::List> oio::List for ThrottleWrapper<R> {
    fn next(&mut self) -> impl Future<Output = Result<Option<oio::Entry>>> + MaybeSend {
        loop {
            if let Err(not_until) = self.limiter.check() {
                let dur = not_until.wait_time_from(self.limiter.clock().now());
                thread::sleep(dur);
                continue;
            }
            break;
        }
        self.inner.next()
    }
}

impl<R: oio::BlockingRead> oio::BlockingRead for ThrottleWrapper<R> {
    fn read(&mut self) -> Result<Buffer> {
        loop {
            if let Err(not_until) = self.limiter.check() {
                let dur = not_until.wait_time_from(self.limiter.clock().now());
                thread::sleep(dur);
                continue;
            }
            break;
        }
        self.inner.read()
    }
}

impl<R: oio::BlockingList> oio::BlockingList for ThrottleWrapper<R> {
    fn next(&mut self) -> Result<Option<oio::Entry>> {
        loop {
            if let Err(not_until) = self.limiter.check() {
                let dur = not_until.wait_time_from(self.limiter.clock().now());
                thread::sleep(dur);
                continue;
            }
            break;
        }
        self.inner.next()
    }
}
