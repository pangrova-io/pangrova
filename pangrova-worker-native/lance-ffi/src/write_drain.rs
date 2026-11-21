//! Local writer ownership evidence, never table or final-object authority.

#[repr(C)]
#[derive(Default)]
pub struct LanceCWriteDrainEvidence {
    pub encoders_and_uploads_joined: u32,
    pub private_stage_cleaned: u32,
    pub staging_publication: u32,
}
