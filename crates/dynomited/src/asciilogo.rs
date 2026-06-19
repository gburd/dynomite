//! ASCII art startup banner.
//!
//! The banner is plain ASCII so it survives the workspace's
//! `check_ascii.sh` gate.

/// Multi-line ASCII art logo printed at startup, written to the log
/// at startup.
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
