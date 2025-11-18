import math

def fit_rayleigh_from_defect_density(densities):
    """
    Ajusta el parámetro sigma de la distribución Rayleigh
    usando defect_density = defects / hours.

    Fórmula derivada del método de máxima verosimilitud para Rayleigh:
    sigma = sqrt( (Σ x^2) / (2n) )
    """
    if len(densities) == 0:
        raise ValueError("No hay densidades para ajustar Rayleigh.")

    squared = [d*d for d in densities]
    sigma = math.sqrt(sum(squared) / (2 * len(densities)))
    return sigma


def rayleigh_pdf(x, sigma):
    return (x / (sigma*sigma)) * math.exp(-(x*x) / (2*sigma*sigma))


def rayleigh_cdf(x, sigma):
    return 1 - math.exp(-(x*x) / (2*sigma*sigma))
