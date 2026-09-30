import { redirect } from 'next/navigation';

// Backward-compatible alias for bookmarks and manually typed legacy URLs.
export default function LegacyWatchlistRedirect() {
  redirect('/watchlists');
}
