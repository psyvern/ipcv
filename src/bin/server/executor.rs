use iced::Executor;
use tokio::runtime::Handle;

pub struct CustomExecutor(Handle);

impl Executor for CustomExecutor {
    fn new() -> Result<Self, futures::io::Error> {
        Handle::try_current()
            .map(Self)
            .map_err(|x| futures::io::Error::new(futures::io::ErrorKind::NotFound, x))
    }

    #[allow(clippy::let_underscore_future)]
    fn spawn(&self, future: impl Future<Output = ()> + Send + 'static) {
        let _ = self.0.spawn(future);
    }

    fn enter<R>(&self, f: impl FnOnce() -> R) -> R {
        let _guard = self.0.enter();
        f()
    }

    fn block_on<T>(&self, future: impl Future<Output = T>) -> T {
        self.0.block_on(future)
    }
}
