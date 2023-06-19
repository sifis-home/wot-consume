use wot_td::{
    extend::ExtendableThing,
    hlist::{self, HListRef, NonEmptyHList},
    protocol::http::{self, HttpProtocol},
};

pub trait HasHttpProtocolExtension: Sized + ExtendableThing {
    fn http_protocol(&self) -> &HttpProtocol;
    fn http_protocol_form(form: &Self::Form) -> &http::Form;
}

impl HasHttpProtocolExtension for HttpProtocol {
    #[inline]
    fn http_protocol(&self) -> &HttpProtocol {
        self
    }

    #[inline]
    fn http_protocol_form(form: &Self::Form) -> &http::Form {
        form
    }
}

macro_rules! impl_hlist_with_http_protocol_in_head {
   (
       @make_hlist
   ) => {
       hlist::Cons<HttpProtocol>
   };

   (
       @make_hlist
       $generic:ident $(, $($rest:tt)*)?
   ) => {
      hlist::Cons<
          $generic,
          impl_hlist_with_http_protocol_in_head!(@make_hlist $($($rest)*)?),
       >
   };

   (
       @make_impl
       $(
           $($generic:ident),+
       )?
   ) => {
       impl $(< $($generic),+ >)? HasHttpProtocolExtension for impl_hlist_with_http_protocol_in_head!(@make_hlist $($($generic),+)?)
       $(
           where
               $(
                   $generic : wot_td::extend::ExtendableThing
               ),+
       )?
       {
           #[inline]
           fn http_protocol(&self) -> &HttpProtocol {
               self.to_ref().split_last().0
           }

           #[inline]
           fn http_protocol_form(form: &Self::Form) -> &http::Form {
               form.to_ref().split_last().0
           }
       }
   };

   (
       $(
           $($generic:ident),* $(,)?
       );+
   ) => {
       $(
           impl_hlist_with_http_protocol_in_head!(@make_impl $($generic),*);
       )*
   };
}

impl_hlist_with_http_protocol_in_head!(
    A;
    A, B;
    A, B, C;
    A, B, C, D;
    A, B, C, D, E;
    A, B, C, D, E, F;
    A, B, C, D, E, F, G;
    A, B, C, D, E, F, G, H;
    A, B, C, D, E, F, G, H, I;
    A, B, C, D, E, F, G, H, I, J;
    A, B, C, D, E, F, G, H, I, J, K;
    A, B, C, D, E, F, G, H, I, J, K, L;
    A, B, C, D, E, F, G, H, I, J, K, L, M;
    A, B, C, D, E, F, G, H, I, J, K, L, M, N;
    A, B, C, D, E, F, G, H, I, J, K, L, M, N, O;
    A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P;
    A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q;
    A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R;
    A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S;
    A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T;
    A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U;
    A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U, V;
    A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U, V, W;
    A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U, V, W, X;
    A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U, V, W, X, Y;
    A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U, V, W, X, Y, Z;
    A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U, V, W, X, Y, Z, AA;
    A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U, V, W, X, Y, Z, AA, AB;
    A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U, V, W, X, Y, Z, AA, AB, AC;
    A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U, V, W, X, Y, Z, AA, AB, AC, AD;
    A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U, V, W, X, Y, Z, AA, AB, AC, AD, AE;
    A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U, V, W, X, Y, Z, AA, AB, AC, AD, AE, AF;
);

#[cfg(test)]
mod tests {
    use wot_td::protocol::coap::CoapProtocol;

    use super::*;

    #[test]
    fn http_protocol() {
        let proto = HttpProtocol {};
        assert!(std::ptr::eq(proto.http_protocol(), &proto));

        let list = hlist::Nil::cons(HttpProtocol {})
            .cons(CoapProtocol {})
            .cons(CoapProtocol {})
            .cons(CoapProtocol {});
        let _http: &HttpProtocol = list.http_protocol();
    }
}
