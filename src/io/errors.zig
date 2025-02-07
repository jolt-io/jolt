//! Possible errors that can be received from I/O utilities.

/// When an error that's not documented is being encountered, callback's return value contains this.
/// Do report unexpected errors please!
pub const UnexpectedError = error{Unexpected};

/// When an operation is cancelled, callback's return value containts this.
pub const CancellationError = error{Cancelled};

pub const ReadError = error{
    EndOfStream,
    NotOpenForReading,
    ConnectionResetByPeer,
    Alignment,
    InputOutput,
    IsDir,
    SystemResources,
    Unseekable,
    ConnectionTimedOut,
} || CancellationError || UnexpectedError;

pub const WriteError = anyerror; // TODO: implementation
pub const ConnectError = anyerror; // TODO: implementation

pub const AcceptError = error{
    WouldBlock,
    ConnectionAborted,
    SocketNotListening,
    ProcessFdQuotaExceeded,
    SystemFdQuotaExceeded,
    SystemResources,
    OperationNotSupported,
    PermissionDenied,
    ProtocolFailure,
} || CancellationError || UnexpectedError;

pub const RecvError = error{
    EndOfStream,
    WouldBlock,
    SystemResources,
    OperationNotSupported,
    PermissionDenied,
    SocketNotConnected,
    ConnectionResetByPeer,
    ConnectionTimedOut,
    ConnectionRefused,
} || CancellationError || UnexpectedError;

/// io_uring only
pub const RecvBufferPoolError = RecvError || error{OutOfBuffers};

pub const SendError = error{
    AccessDenied,
    WouldBlock,
    FastOpenAlreadyInProgress,
    AddressFamilyNotSupported,
    FileDescriptorInvalid,
    ConnectionResetByPeer,
    MessageTooBig,
    SystemResources,
    SocketNotConnected,
    FileDescriptorNotASocket,
    OperationNotSupported,
    BrokenPipe,
    ConnectionTimedOut,
} || CancellationError || UnexpectedError;

pub const TimeoutError = CancellationError || UnexpectedError;

/// io_uring only
/// NOTE: this can be cancellable too I believe
pub const UpdateFdsError = error{
    OutOfMemory,
    InvalidArguments,
    UnableToCopyFileDescriptors,
    InvalidFileDescriptor,
    FileDescriptorOverflow,
} || UnexpectedError;
