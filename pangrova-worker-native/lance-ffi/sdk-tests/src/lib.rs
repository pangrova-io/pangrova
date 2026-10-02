//! Independent tests for the repository-owned, pinned SDK cancellation contract.
//! These are component tests, not SQL or multi-process deployment acceptance.
#[cfg(test)]
mod fixture;
#[cfg(test)]
mod dataset_tests;
#[cfg(test)]
mod remote_fixture;
#[cfg(test)]
mod remote_tests;
#[cfg(test)]
mod writer_tests;
