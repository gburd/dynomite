//! ASCII art startup banner.
//!
//! The banner is reproduced from the reference engine's
//! `dyn_asciilogo.h` after the C compiler folds away unrecognised
//! escape sequences. The result is plain ASCII so it survives the
//! workspace's `check_ascii.sh` gate.

/// Multi-line ASCII art logo printed at startup, identical to the
/// banner the reference engine emits via `loga("%s", ascii_logo)`.
///
/// # Examples
///
/// ```
/// assert!(dynomited::asciilogo::ASCII_LOGO.contains("mmm#"));
/// assert!(dynomited::asciilogo::ASCII_LOGO.is_ascii());
/// ```
pub const ASCII_LOGO: &str = concat!(
    "                                                                      \n",
    "     #                                      m                        \n",
    "  mmm#  m   m  mmmm    mmm   mmmmm  mmm    mm#mm   mmm                \n",
    " #   #  \\m m/  #   #  #   #  # # #    #      #    #   #               \n",
    " #   #   #m#   #   #  #   #  # # #    #      #    #''''               \n",
    " \\#m##   \\#    #   #   #m#   # # #  mm#mm    mm    #mm                \n",
    "         m/\n",
    "        ##\n",
);
