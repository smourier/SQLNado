using System.Diagnostics.CodeAnalysis;

[assembly: SuppressMessage("Design", "CA1031:Do not catch general exception types", Justification = "Well, sometimes, you have no choice.")]
[assembly: SuppressMessage("Performance", "CA1819:Properties should not return arrays", Justification = "Kept for compat reasons.")]
[assembly: SuppressMessage("Style", "IDE0016:Use 'throw' expression", Justification = "Don't like that.")]
