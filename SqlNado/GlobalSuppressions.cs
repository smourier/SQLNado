using System.Diagnostics.CodeAnalysis;

[assembly: SuppressMessage("Design", "CA1031:Do not catch general exception types", Justification = "Well, sometimes, you have no choice.")]
[assembly: SuppressMessage("Performance", "CA1819:Properties should not return arrays", Justification = "Kept for compat reasons.")]
[assembly: SuppressMessage("Style", "IDE0016:Use 'throw' expression", Justification = "Don't like that.")]
[assembly: SuppressMessage("Style", "IDE0270:Use coalesce expression", Justification = "Not fan of this.")]
