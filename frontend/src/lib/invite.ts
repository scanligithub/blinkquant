export function parseInviteCodes(envValue: string | undefined): string[] {
  if (!envValue) return [];
  return envValue.split(',').map((s) => s.trim()).filter(Boolean);
}

export function isValidInviteCode(
  inviteCodes: string[],
  code: string | undefined | null
): boolean {
  if (inviteCodes.length === 0) return true;
  return typeof code === 'string' && inviteCodes.includes(code);
}
