package commands

// ClassifiedError carries stable machine-readable error metadata without
// changing the legacy JSON envelope used by command transports.
type ClassifiedError struct {
	Code string
	Category string
	Message string
	Retryable bool
	Cause error
}

func (e *ClassifiedError) Error() string {
	if e == nil {
		return ""
	}
	if e.Message != "" {
		return e.Message
	}
	if e.Cause != nil {
		return e.Cause.Error()
	}
	return e.Code
}

func (e *ClassifiedError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Cause
}

func NewClassifiedError(code, category, message string, retryable bool, cause error) error {
	return &ClassifiedError{
		Code: code, Category: category, Message: message,
		Retryable: retryable, Cause: cause,
	}
}
