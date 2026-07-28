//! Worker-local exchange buffering and reservation shell.

use crate::errors::ExecutionError;

/// One logical exchange channel inside a worker-local runtime.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ExchangeChannel {
    pub channel_id: String,
    pub partition_count: u32,
}

/// Reservation request for exchange-side memory or spill budget.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ExchangeReservation {
    pub reservation_id: String,
    pub requested_bytes: u64,
}

/// Worker-local exchange buffer boundary.
pub trait ExchangeBufferManager {
    fn open_exchange_channel(
        &self,
        channel: ExchangeChannel,
    ) -> Result<ExchangeChannel, ExecutionError>;

    fn reserve_exchange_capacity(
        &self,
        reservation: ExchangeReservation,
    ) -> Result<ExchangeReservation, ExecutionError>;
}

#[cfg(test)]
mod tests {
    use super::{ExchangeBufferManager, ExchangeChannel, ExchangeReservation};

    #[test]
    fn exchange_buffer_shell_carries_channel_and_reservation_shape() {
        let channel = ExchangeChannel {
            channel_id: "exchange-a".to_owned(),
            partition_count: 16,
        };
        let reservation = ExchangeReservation {
            reservation_id: "reserve-a".to_owned(),
            requested_bytes: 1_048_576,
        };

        assert_eq!(channel.partition_count, 16);
        assert_eq!(reservation.requested_bytes, 1_048_576);
        let _ = Option::<&dyn ExchangeBufferManager>::None;
    }
}
