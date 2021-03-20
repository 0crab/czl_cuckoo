#ifndef HASHCOMP_GENERATOR_H
#define HASHCOMP_GENERATOR_H

#include <algorithm>
#include <cmath>
#include <random>

/** Zipf-like random distribution.
 *
 * "Rejection-inversion to generate variates from monotone discrete
 * distributions", Wolfgang Hörmann and Gerhard Derflinger
 * ACM TOMACS 6.3 (1996): 169-184
 */
template<class IntType = unsigned long, class RealType = double>
class zipf_distribution {
public:
    static constexpr RealType epsilon = 1e-8;
    typedef RealType input_type;
    typedef IntType result_type;

    static_assert(std::numeric_limits<IntType>::is_integer, "");
    static_assert(!std::numeric_limits<RealType>::is_integer, "");

    zipf_distribution(const IntType n = std::numeric_limits<IntType>::max(),
                      const RealType q = 1.0)
            : n(n), q(q), H_x1(H(1.5) - 1.0), H_n(H(n + 0.5)), dist(H_x1, H_n) {}

    IntType operator()(std::mt19937 &rng) {
        while (true) {
            const RealType u = dist(rng);
            const RealType x = H_inv(u);
            const IntType k = clamp<IntType>(std::round(x), 1, n);
            if (u >= H(k + 0.5) - h(k)) {
                return k;
            }
        }
    }

private:
    /** Clamp x to [min, max]. */
    template<typename T>
    static constexpr T clamp(const T x, const T min, const T max) {
        return std::max(min, std::min(max, x));
    }

    /** exp(x) - 1 / x */
    static double expxm1bx(const double x) {
        return (std::abs(x) > epsilon)
               ? std::expm1(x) / x
               : (1.0 + x / 2.0 * (1.0 + x / 3.0 * (1.0 + x / 4.0)));
    }

    /** H(x) = log(x) if q == 1, (x^(1-q) - 1)/(1 - q) otherwise.
     * H(x) is an integral of h(x).
     *
     * Note the numerator is one less than in the paper order to work with all
     * positive q.
     */
    const RealType H(const RealType x) {
        const RealType log_x = std::log(x);
        return expxm1bx((1.0 - q) * log_x) * log_x;
    }

    /** log(1 + x) / x */
    static RealType
    log1pxbx(const RealType x) {
        return (std::abs(x) > epsilon)
               ? std::log1p(x) / x
               : 1.0 - x * ((1 / 2.0) - x * ((1 / 3.0) - x * (1 / 4.0)));
    }

    /** The inverse function of H(x) */
    const RealType H_inv(const RealType x) {
        const RealType t = std::max(-1.0, x * (1.0 - q));
        return std::exp(log1pxbx(t) * x);
    }

    /** That hat function h(x) = 1 / (x ^ q) */
    const RealType h(const RealType x) {
        return std::exp(-q * std::log(x));
    }

    IntType n;     ///< Number of elements
    RealType q;     ///< Exponent
    RealType H_x1;  ///< H(x_1)
    RealType H_n;   ///< H(n)
    std::uniform_real_distribution<RealType> dist;  ///< [H(x_1), H(n)]
};

#define FUZZY_BOUND 0

template<typename R>
class RandomGenerator {
public:
    static inline void generate(R *array, size_t range, size_t count, double skew = 0.0) {
        //struct stat buffer;
        if (skew < zipf_distribution<R>::epsilon) {
            std::default_random_engine engine(
                    static_cast<R>(std::chrono::steady_clock::now().time_since_epoch().count()));
            std::uniform_int_distribution<size_t> dis(0, range + FUZZY_BOUND);
            for (size_t i = 0; i < count; i++) {
                array[i] = static_cast<R>(dis(engine));
            }
        } else {
            zipf_distribution<R> engine(range, skew);
            std::mt19937 mt;
            for (size_t i = 0; i < count; i++) {
                array[i] = engine(mt);
            }
        }
    }
};
#endif //HASHCOMP_GENERATOR_H
